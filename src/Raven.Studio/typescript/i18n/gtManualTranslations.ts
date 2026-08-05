import { hashSource } from "generaltranslation/id";
import type { JsxChildren } from "generaltranslation/types";

/**
 * Hand-written GT translations.
 *
 * GT normally keys translations by a content hash produced by `npx gt translate`,
 * which uploads source strings to General Translation's service. The same hash is
 * computable locally with `hashSource`, so translations can be authored by hand
 * and the service skipped entirely - no account, no API key, and source strings
 * never leave the repo.
 *
 * Verified against real `gt translate` output: dataFormat "JSX" is the variant GT
 * looks up at runtime (see gtManualTranslations.spec.ts).
 */
export type GtSource = JsxChildren | string;

export interface ManualEntry {
    /** English source, matching the <T> contents exactly. */
    source: GtSource;
    /** Translation per locale. */
    translations: Record<string, GtSource>;
}

export function gtKey(source: GtSource): string {
    return hashSource({ source, dataFormat: "JSX" });
}

/**
 * Builds the object GT's loadTranslations must return for a locale. Entries with
 * no translation for that locale are omitted so GT falls back to English rather
 * than rendering nothing.
 */
export function buildTranslations(entries: ManualEntry[], locale: string): Record<string, GtSource> {
    const result: Record<string, GtSource> = {};

    for (const entry of entries) {
        const translation = entry.translations[locale];
        if (translation !== undefined) {
            result[gtKey(entry.source)] = translation;
        }
    }

    return result;
}
