import i18next from "i18next";
import { defaultLocale } from "./config";

describe("i18n lazy loading", () => {
    afterEach(async () => {
        await i18next.changeLanguage(defaultLocale);
    });

    it("loads only the shared namespace up front", () => {
        // "common" is declared in init; per-view namespaces must not be bundled
        // with it, otherwise nothing is actually deferred.
        expect(i18next.options.ns).toEqual(["common"]);
        expect(i18next.hasResourceBundle(defaultLocale, "common")).toBe(true);
    });

    it("fetches a view namespace on demand", async () => {
        const namespace = "documentExpiration";

        // Drop it so this test observes a real fetch even after other specs ran.
        i18next.removeResourceBundle(defaultLocale, namespace);
        expect(i18next.hasResourceBundle(defaultLocale, namespace)).toBe(false);

        await i18next.loadNamespaces(namespace);

        expect(i18next.hasResourceBundle(defaultLocale, namespace)).toBe(true);
        expect(i18next.t(($) => $.header, { ns: namespace })).toBe("Document Expiration");
    });

    it("fetches each locale's namespace separately", async () => {
        i18next.removeResourceBundle("pl", "documentExpiration");

        await i18next.changeLanguage("pl");
        await i18next.loadNamespaces("documentExpiration");

        expect(i18next.hasResourceBundle("pl", "documentExpiration")).toBe(true);
        expect(i18next.t(($) => $.header, { ns: "documentExpiration" })).toBe("Wygasanie dokumentów");
    });
});
