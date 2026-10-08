#ifndef TCP_SERVER_H
#define TCP_SERVER_H

#include "ServerMessage.h"

#include <atomic>
#include <deque>
#include <memory>
#include <thread>
#include <vector>

#include "asio.hpp"

namespace DataExporter
{
    class TCPServerConnection : public std::enable_shared_from_this<TCPServerConnection>
    {
    public:
        using Ref = std::shared_ptr<TCPServerConnection>;

        explicit TCPServerConnection(asio::ip::tcp::socket socket);
        ~TCPServerConnection();

        bool IsConnected() const { return m_socket.is_open(); }

        // These functions are called only from the server io_context thread.
        void Send(const ServerMessage &message);
        void Disconnect();

    private:
        void WriteHeader();
        void WriteBody();

        asio::ip::tcp::socket m_socket;
        std::deque<ServerMessage> m_queue;
        std::shared_ptr<uint64_t> m_activeHeader;
    };

    class TCPServer
    {
    public:
        explicit TCPServer(uint16_t serverPort);
        ~TCPServer();

        void StartServer();
        void ShutdownServer();
        void MessageClients(const ServerMessage &message);

        bool IsActive() const { return m_running.load(); }

    private:
        void WaitForClient();
        void MessageClientsOnIoThread(const ServerMessage &message);

        uint16_t m_acceptorPort;
        asio::io_context m_context;
        asio::ip::tcp::acceptor m_acceptor;
        std::vector<TCPServerConnection::Ref> m_clients;
        std::thread m_ioThread;
        std::atomic<bool> m_running;
    };
}

#endif
