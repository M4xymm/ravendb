import type { ManualEntry } from "../gtManualTranslations";

/**
 * Hand-written translations for the Document Expiration view.
 *
 * `source` must match the <T> contents exactly - it is hashed to produce the key
 * GT looks up, so any difference (including wording or punctuation) silently
 * misses and falls back to English. The accompanying spec guards against that by
 * asserting each key resolves.
 *
 * Nested JSX (the About section) uses GT's structural form: `t` is the tag, `c`
 * its children, and `i` the element index. Indices must match the source tree.
 */
export const documentExpirationEntries: ManualEntry[] = [
    {
        source: "Document Expiration",
        translations: { pl: "Wygasanie dokumentów" },
    },
    {
        source: "Save",
        translations: { pl: "Zapisz" },
    },
    {
        source: "Enable Document Expiration",
        translations: { pl: "Włącz wygasanie dokumentów" },
    },
    {
        source: "Set custom expiration frequency",
        translations: { pl: "Ustaw własną częstotliwość wygasania" },
    },
    {
        source: "Set max number of documents to process in a single run",
        translations: { pl: "Ustaw maksymalną liczbę dokumentów przetwarzanych w jednym przebiegu" },
    },
    {
        source: "useful links",
        translations: { pl: "przydatne odnośniki" },
    },
];
