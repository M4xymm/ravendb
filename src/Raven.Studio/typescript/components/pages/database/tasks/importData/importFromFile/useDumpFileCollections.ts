import { useEffect, useState } from "react";
import { Decompress } from "fzstd";

interface DumpFileCollectionsState {
    collections: string[];
    isReading: boolean;
    readError: string;
}

const initialState: DumpFileCollectionsState = {
    collections: [],
    isReading: false,
    readError: null,
};

// Matches "@collection":"<name>" including escaped characters inside the name
const collectionRegex = /"@collection"\s*:\s*"((?:[^"\\]|\\.)*)"/g;

// Keep a tail between chunks so a match split across chunk boundary is not lost
const chunkOverlap = 256;

async function scanStream(stream: ReadableStream<Uint8Array>, signal: AbortSignal): Promise<string[]> {
    const decoder = new TextDecoder();
    const reader = stream.getReader();
    const found = new Set<string>();
    let carry = "";

    // eslint-disable-next-line no-constant-condition
    while (true) {
        if (signal.aborted) {
            await reader.cancel();
            throw new DOMException("Aborted", "AbortError");
        }

        const { done, value } = await reader.read();
        if (done) {
            break;
        }

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
function zstdDecompressStream(input: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> {
    const reader = input.getReader();
    let decompress: Decompress;
    let isClosed = false;

    return new ReadableStream<Uint8Array>({
        start(controller) {
            decompress = new Decompress((chunk, isLast) => {
                if (isClosed) {
                    return;
                }
                if (chunk.length > 0) {
                    controller.enqueue(chunk);
                }
                if (isLast) {
                    isClosed = true;
                    controller.close();
                }
            });
        },
        async pull(controller) {
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
            decompress.push(value);
        },
        cancel(reason) {
            isClosed = true;
            return reader.cancel(reason);
        },
    });
}

async function readCollectionsFromDumpFile(file: File, signal: AbortSignal): Promise<string[]> {
    const header = new Uint8Array(await file.slice(0, 4).arrayBuffer());
    const isGzip = header.length >= 2 && header[0] === 0x1f && header[1] === 0x8b;
    const isZstd =
        header.length === 4 &&
        header[0] === 0x28 &&
        header[1] === 0xb5 &&
        header[2] === 0x2f &&
        header[3] === 0xfd;

    let stream: ReadableStream<Uint8Array>;
    if (isGzip) {
        stream = file.stream().pipeThrough(new DecompressionStream("gzip"));
    } else if (isZstd) {
        stream = zstdDecompressStream(file.stream());
    } else {
        stream = file.stream();
    }

    return scanStream(stream, signal);
}

export function useDumpFileCollections(file: File | null): DumpFileCollectionsState {
    const [state, setState] = useState<DumpFileCollectionsState>(initialState);

    useEffect(() => {
        if (!file) {
            setState(initialState);
            return;
        }

        const abortController = new AbortController();
        setState({ collections: [], isReading: true, readError: null });

        readCollectionsFromDumpFile(file, abortController.signal)
            .then((collections) => {
                if (!abortController.signal.aborted) {
                    setState({ collections, isReading: false, readError: null });
                }
            })
            .catch((error) => {
                if (!abortController.signal.aborted) {
                    setState({
                        collections: [],
                        isReading: false,
                        readError:
                            "Could not read the collection list from the selected file" +
                            (error instanceof Error && error.message ? ` (${error.message})` : ""),
                    });
                }
            });

        return () => abortController.abort();
    }, [file]);

    return state;
}
