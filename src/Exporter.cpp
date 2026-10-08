/** @file:  Exporter.cpp
 *  @brief: Implements a TCP server fro GRAW Frames
 */

#include "Exporter.h"

#include <mfm/Common.h>
#include <mfm/Field.h>
#include <mfm/Frame.h>
#include <mfm/Header.h>

#include "server/ServerMessage.h"

Exporter::Exporter(uint16_t serverPort) : FrameStorage(), m_server(serverPort) {
    m_server.StartServer();
}

Exporter::~Exporter() {}

void Exporter::processFrame(mfm::Frame &frame) {
    // Get the frame size, and pointer to start
    size_t nbytes = sizeFrame(frame);
    const mfm::Byte *pData = frame.data();

    // Write the frame to disk first.
    FrameStorage::processFrame(frame);

    // Forward a copy to connected TCP clients.  MessageClients() is the only
    // cross-thread boundary; all socket/client state is owned by the server's
    // io_context thread.
    if (m_server.IsActive()) {
        std::vector<uint8_t> buffer;
        buffer.insert(buffer.begin(), ((uint8_t *)pData), ((uint8_t *)pData + nbytes));
        m_server.MessageClients(DataExporter::ServerMessage(buffer));
    }
}

/*-----------------------------------------------------------------------------
 *  Private utilities.
 */
/**
 * sizeFrame
 *    If I understand the Frame class correctly,   What we need to do
 *    is get a const reference to the header (supported by Frame).  Ask the
 *    header to tell us its size and the size of the frame body that follows.
 *
 *  @param frame - const reference to the frame we're dissecting.
 *  @return size_t - size of the frame + header all in one.  I hope this
 *                   includes the meta type field. else we'll miss a byte.
 */
size_t Exporter::sizeFrame(const mfm::Frame &frame) {
    mfm::Header const &h = frame.header();
    size_t result = h.headerSize_B() + h.dataSize_B();

    return result;
}
