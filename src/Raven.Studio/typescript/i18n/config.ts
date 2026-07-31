import i18next from "i18next";
import { initReactI18next } from "react-i18next";
import Pseudo from "i18next-pseudo";
import resourcesToBackend from "i18next-resources-to-backend";

// Type-only imports: erased at build time, so the English files below are typed
// against without being bundled. Every locale is fetched on demand instead.
import type common from "./locales/en/common.json";
import type documentExpiration from "./locales/en/documentExpiration.json";

declare module "i18next" {
    interface CustomTypeOptions {
        defaultNS: "common";
        resources: {
            common: typeof common;
            documentExpiration: typeof documentExpiration;
        };
        enableSelector: "optimize";
    }
}

export const pseudoLocale = "en-XA";
export const defaultLocale = "en";

/**
 * Locales offered in the UI. The pseudo locale is a translation-coverage tool,
 * not a real language: it accents and lengthens English so any string that
 * bypassed the translation layer stands out.
 */
export const supportedLocales = [
    { code: defaultLocale, label: "English" },
    { code: "pl", label: "Polski" },
    { code: pseudoLocale, label: "Pseudo (testing)" },
];

/**
 * Loads one namespace for one locale on demand.
 *
 * webpack turns this dynamic import into a separate chunk per file, so a view's
 * translations are fetched when the view first needs them rather than shipped in
 * the main bundle. The pseudo locale has no files of its own - it post-processes
 * English - so it reads the English resources.
 */
const backend = resourcesToBackend(async (language: string, namespace: string) => {
    const locale = language === pseudoLocale ? defaultLocale : language;
    return (await import(`./locales/${locale}/${namespace}.json`)).default;
});

export function initI18n(language: string = defaultLocale): Promise<unknown> {
    if (i18next.isInitialized) {
        return Promise.resolve(i18next);
    }

    return i18next
        .use(backend)
        .use(
            new Pseudo({
                enabled: true,
                languageToPseudo: pseudoLocale,
                wrapped: true,
            })
        )
        .use(initReactI18next)
        .init({
            lng: language,
            fallbackLng: defaultLocale,
            defaultNS: "common",
            // Loaded up front so shared strings never suspend a view.
            ns: ["common"],
            interpolation: {
                // React already escapes rendered values
                escapeValue: false,
            },
            postProcess: ["pseudo"],
            react: {
                // Suspend rendering until a namespace resolves, so views never
                // flash raw keys or source-language text.
                useSuspense: true,
            },
        });
}
