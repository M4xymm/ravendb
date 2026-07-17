import { Decompress } from "fzstd";

// Scans a .ravendbdump file for collection names off the main thread.
// Input message: { file: File }
// Output messages: { type: "progress", percent } | { type: "result", collections } | { type: "error", message }

export interface DumpScanRequest {
    file: File;
}

export type DumpScanResponse =
    | { type: "progress"; percent: number }
    | { type: "result"; collections: string[] }
    | { type: "error"; message: string };

// Matches "@collection":"<name>" including escaped characters inside the name
const collectionRegex = /"@collection"\s*:\s*"((?:[^"\\]|\\.)*)"/g;

// Keep a tail between chunks so a match split across a chunk boundary is not lost
const chunkOverlap = 256;

const workerContext = self as unknown as Worker;

function post(message: DumpScanResponse) {
    workerContext.postMessage(message);
}

async function scanStream(stream: ReadableStream<Uint8Array>, onBytes: (bytes: number) => void): Promise<string[]> {
    const decoder = new TextDecoder();
    const reader = stream.getReader();
    const found = new Set<string>();
    let carry = "";

    // eslint-disable-next-line no-constant-condition
    while (true) {
        const { done, value } = await reader.read();
        if (done) {
            break;
        }

        onBytes(value.byteLength);

        const text = carry + decoder.decode(value, { stream: true });

        let match: RegExpExecArray;
        collectionRegex.lastIndex = 0;
        while ((match = collectionRegex.exec(text)) !== null) {
            try {
                found.add(JSON.parse(`"${match[1]}"`));
            } catch {
                // malformed escape sequence - skip
            }
        }

        carry = text.slice(-chunkOverlap);
    }

    return Array.from(found).sort((a, b) => a.localeCompare(b));
}

// The browser's DecompressionStream doesn't support zstd, which is the server's default
// export compression - fzstd (pure JS) handles that branch.
function zstdDecompressStream(
    input: ReadableStream<Uint8Array>,
    onCompressedBytes: (bytes: number) => void
): ReadableStream<Uint8Array> {
    const reader = input.getReader();
    let decompress: Decompress;
    let isClosed = false;
    let hasProducedInPull = false;

    return new ReadableStream<Uint8Array>({
        start(controller) {
            decompress = new Decompress((chunk, isLast) => {
                if (isClosed) {
                    return;
                }
                if (chunk.length > 0) {
                    hasProducedInPull = true;
                    controller.enqueue(chunk);
                }
                if (isLast) {
                    isClosed = true;
                    controller.close();
                }
            });
        },
        async pull(controller) {
            // keep feeding the decompressor until it produces output: the stream never calls
            // pull again after a pull that enqueued nothing, and fzstd buffers input internally
            // until it can complete a block - a single read per pull would deadlock
            hasProducedInPull = false;
            while (!isClosed && !hasProducedInPull) {
                const { done, value } = await reader.read();
                if (done) {
                    if (!isClosed) {
                        decompress.push(new Uint8Array(0), true);
                        if (!isClosed) {
                            isClosed = true;
                            controller.close();
                        }
                    }
                    return;
                }
                onCompressedBytes(value.byteLength);
                decompress.push(value);
            }
        },
        cancel(reason) {
            isClosed = true;
            return reader.cancel(reason);
        },
    });
}

async function readCollectionsFromDumpFile(file: File): Promise<string[]> {
    const header = new Uint8Array(await file.slice(0, 4).arrayBuffer());
    const isGzip = header.length >= 2 && header[0] === 0x1f && header[1] === 0x8b;
    const isZstd =
        header.length === 4 &&
        header[0] === 0x28 &&
        header[1] === 0xb5 &&
        header[2] === 0x2f &&
        header[3] === 0xfd;

    // progress tracks consumed COMPRESSED bytes against the file size
    let bytesRead = 0;
    let lastReportedPercent = -1;
    const reportCompressedBytes = (bytes: number) => {
        bytesRead += bytes;
        const percent = Math.min(100, Math.floor((bytesRead / file.size) * 100));
        if (percent !== lastReportedPercent) {
            lastReportedPercent = percent;
            post({ type: "progress", percent });
        }
    };

    let stream: ReadableStream<Uint8Array>;
    if (isGzip) {
        // no per-chunk hook on DecompressionStream input here - report on decompressed output instead,
        // approximating with a 1:3 compression ratio floor just for the progress bar
        stream = file.stream().pipeThrough(new DecompressionStream("gzip"));
    } else if (isZstd) {
        stream = zstdDecompressStream(file.stream(), reportCompressedBytes);
    } else {
        stream = file.stream();
    }

    const isCompressedProgress = isZstd;
    return scanStream(stream, (bytes) => {
        if (!isCompressedProgress) {
            // plain files: exact; gzip: decompressed bytes overshoot the file size - clamp handles it
            reportCompressedBytes(bytes);
        }
    });
}

workerContext.onmessage = async (event: MessageEvent<DumpScanRequest>) => {
    try {
        const collections = await readCollectionsFromDumpFile(event.data.file);
        post({ type: "result", collections });
    } catch (error) {
        post({ type: "error", message: error instanceof Error ? error.message : String(error) });
    }
};
