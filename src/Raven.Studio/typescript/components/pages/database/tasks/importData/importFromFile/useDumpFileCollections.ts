import { useEffect, useState } from "react";
import type { DumpScanResponse } from "workers/dumpCollectionsScanWorker";

interface DumpFileCollectionsState {
    collections: string[];
    isReading: boolean;
    readProgressPercent: number;
    readError: string;
}

const initialState: DumpFileCollectionsState = {
    collections: [],
    isReading: false,
    readProgressPercent: 0,
    readError: null,
};

// The scan (zstd/gzip decompression + regex over the whole dump) is CPU-heavy, so it runs in a
// dedicated web worker (see typescript/workers/dumpCollectionsScanWorker.ts, bundled via its own
// webpack entry) - large files would freeze the UI if scanned on the main thread.
export function useDumpFileCollections(file: File | null): DumpFileCollectionsState {
    const [state, setState] = useState<DumpFileCollectionsState>(initialState);

    useEffect(() => {
        if (!file) {
            setState(initialState);
            return;
        }

        setState({ collections: [], isReading: true, readProgressPercent: 0, readError: null });

        const worker = new Worker("/studio/assets/dump_scan_worker.js");

        worker.onmessage = (event: MessageEvent<DumpScanResponse>) => {
            const message = event.data;
            switch (message.type) {
                case "progress":
                    setState((prev) => ({ ...prev, readProgressPercent: message.percent }));
                    break;
                case "result":
                    setState({
                        collections: message.collections,
                        isReading: false,
                        readProgressPercent: 100,
                        readError: null,
                    });
                    break;
                case "error":
                    setState({
                        collections: [],
                        isReading: false,
                        readProgressPercent: 0,
                        readError:
                            "Could not read the collection list from the selected file" +
                            (message.message ? ` (${message.message})` : ""),
                    });
                    break;
            }
        };

        worker.onerror = () => {
            setState({
                collections: [],
                isReading: false,
                readProgressPercent: 0,
                readError: "Could not read the collection list from the selected file",
            });
        };

        worker.postMessage({ file });

        return () => worker.terminate();
    }, [file]);

    return state;
}
