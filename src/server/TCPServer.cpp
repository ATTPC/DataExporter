#include "TCPServer.h"

#include <algorithm>
#include <iostream>

namespace DataExporter
{
    TCPServerConnection::TCPServerConnection(asio::ip::tcp::socket socket)
        : m_socket(std::move(socket))
    {
    }

    TCPServerConnection::~TCPServerConnection()
    {
        std::error_code ec;
        m_socket.close(ec);
    }

    void TCPServerConnection::Disconnect()
    {
        // Connection objects are owned and manipulated by the io_context thread.
        std::error_code ec;
        m_socket.close(ec);
        m_queue.clear();
        m_activeHeader.reset();
    }

    void TCPServerConnection::Send(const ServerMessage &message)
    {
        if (!m_socket.is_open())
            return;

        const bool wasEmpty = m_queue.empty();
        m_queue.push_back(message);

        if (wasEmpty)
            WriteHeader();
    }

    void TCPServerConnection::WriteHeader()
    {
        if (m_queue.empty() || !m_socket.is_open())
            return;

        // The header must remain alive until async_write completes.
        m_activeHeader = std::make_shared<uint64_t>(m_queue.front().size);
        Ref self = shared_from_this();

        asio::async_write(m_socket,
                          asio::buffer(m_activeHeader.get(), sizeof(*m_activeHeader)),
                          [self](std::error_code ec, std::size_t /*length*/)
                          {
                              if (ec)
                              {
                                  if (ec != asio::error::operation_aborted)
                                      std::cerr << "TCPServerConnection -> Failure to write header: "
                                                << ec.message() << std::endl;
                                  self->Disconnect();
                                  return;
                              }

                              self->m_activeHeader.reset();

                              if (self->m_queue.empty())
                                  return;

                              if (self->m_queue.front().size > 0)
                                  self->WriteBody();
                              else
                              {
                                  self->m_queue.pop_front();
                                  if (!self->m_queue.empty())
                                      self->WriteHeader();
                              }
                          });
    }

    void TCPServerConnection::WriteBody()
    {
        if (m_queue.empty() || !m_socket.is_open())
            return;

        Ref self = shared_from_this();
        ServerMessage &message = m_queue.front();

        // The front message remains in m_queue until this operation completes,
        // so its vector storage remains valid for the complete async write.
        asio::async_write(m_socket,
                          asio::buffer(message.body.data(), message.body.size()),
                          [self](std::error_code ec, std::size_t /*length*/)
                          {
                              if (ec)
                              {
                                  if (ec != asio::error::operation_aborted)
                                      std::cerr << "TCPServerConnection -> Failure to write body: "
                                                << ec.message() << std::endl;
                                  self->Disconnect();
                                  return;
                              }

                              if (!self->m_queue.empty())
                                  self->m_queue.pop_front();

                              if (!self->m_queue.empty())
                                  self->WriteHeader();
                          });
    }

    TCPServer::TCPServer(uint16_t serverPort)
        : m_acceptorPort(serverPort),
          m_context(),
          m_acceptor(m_context, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), serverPort)),
          m_running(false)
    {
    }

    TCPServer::~TCPServer()
    {
        ShutdownServer();
    }

    void TCPServer::StartServer()
    {
        if (m_running.exchange(true))
            return;

        try
        {
            WaitForClient();
            m_ioThread = std::thread([this]()
                                     {
                                         try
                                         {
                                             m_context.run();
                                         }
                                         catch (const std::exception &e)
                                         {
                                             std::cerr << "TCP server I/O thread exception: "
                                                       << e.what() << std::endl;
                                         }
                                     });
        }
        catch (const std::exception &e)
        {
            m_running.store(false);
            std::cerr << "Server caught exception: " << e.what() << std::endl;
            return;
        }

        std::cout << "Server has started and is listening for clients on port "
                  << m_acceptorPort << std::endl;
    }

    void TCPServer::ShutdownServer()
    {
        // This gate is checked before MessageClients() posts into the context.
        // exchange() also makes repeated shutdown calls harmless.
        if (!m_running.exchange(false))
            return;

        // Stop accepting immediately. Existing handlers will either complete or
        // be abandoned when the context is stopped below.
        std::error_code ec;
        m_acceptor.close(ec);

        m_context.stop();

        if (m_ioThread.joinable())
            m_ioThread.join();

        // No io_context handler can touch the client vector after join().
        m_clients.clear();

        std::cout << "Server has been shutdown." << std::endl;
    }

    void TCPServer::MessageClients(const ServerMessage &message)
    {
        // This is the only cross-thread boundary. Never touch m_clients or a
        // connection from the acquisition thread.
        if (!m_running.load())
            return;

        asio::post(m_context,
                   [this, message]()
                   {
                       // A message may have been queued immediately before shutdown.
                       if (!m_running.load())
                           return;

                       MessageClientsOnIoThread(message);
                   });
    }

    void TCPServer::MessageClientsOnIoThread(const ServerMessage &message)
    {
        for (std::vector<TCPServerConnection::Ref>::iterator it = m_clients.begin();
             it != m_clients.end();)
        {
            TCPServerConnection::Ref &client = *it;

            if (client && client->IsConnected())
            {
                // Send() is deliberately synchronous with respect to the
                // io_context thread: it only queues the message and starts an
                // asynchronous socket write when necessary.
                client->Send(message);
                ++it;
            }
            else
            {
                if (client)
                    client->Disconnect();
                it = m_clients.erase(it);
            }
        }
    }

    void TCPServer::WaitForClient()
    {
        if (!m_running.load() || !m_acceptor.is_open())
            return;

        m_acceptor.async_accept(
            [this](std::error_code ec, asio::ip::tcp::socket socket)
            {
                if (!m_running.load())
                    return;

                if (!ec)
                {
                    std::cout << "Server connection to new client: "
                              << socket.remote_endpoint() << std::endl;
                    m_clients.push_back(
                        std::make_shared<TCPServerConnection>(std::move(socket)));
                }
                else if (ec != asio::error::operation_aborted)
                {
                    std::cerr << "Server Connection Failure with error code: "
                              << ec.message() << std::endl;
                }

                if (m_running.load() && m_acceptor.is_open())
                    WaitForClient();
            });
    }
}
