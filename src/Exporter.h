#ifndef EXPORT_PROCESSOR_H
#define EXPORT_PROCESSOR_H

// This has to be included first, as it defines all of the awful macros

#include "BadMacros.h"

#include <get/daq/FrameStorage.h>
#include "server/TCPServer.h"

#include <string>
#include <memory>

class Exporter : public get::daq::FrameStorage
{
public:
    Exporter(uint16_t serverPort);
    virtual ~Exporter();

    // overrides from FrameStorage
    virtual void processFrame(mfm::Frame &frame) override;

    // This is just done to disable the debugging log
    // message from each frame header
    void processHeader(const mfm::PrimaryHeader &header) {}

private:
    // Forbidden constructors
    Exporter(const Exporter &r);
    Exporter &operator=(const Exporter &r);
    // Utility
    size_t sizeFrame(const mfm::Frame &frame);

    DataExporter::TCPServer m_server;
};

#endif
