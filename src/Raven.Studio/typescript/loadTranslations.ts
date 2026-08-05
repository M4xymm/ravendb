import { buildTranslations } from "./i18n/gtManualTranslations";
import { documentExpirationEntries } from "./i18n/locales/documentExpirationEntries";

/**
 * Supplies GT with hand-written translations, keyed by locally computed content
 * hashes. Nothing is fetched and `npx gt translate` is never run, so source
 * strings stay in the repo.
 */
export default async function loadTranslations(locale: string) {
    return buildTranslations(documentExpirationEntries, locale);
}
