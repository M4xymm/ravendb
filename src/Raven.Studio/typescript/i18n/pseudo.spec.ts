import i18next from "i18next";
import { pseudoLocale } from "./config";

describe("i18n pseudo-localization", () => {
    afterEach(async () => {
        await i18next.changeLanguage("en");
    });

    it("renders real English by default", async () => {
        await i18next.loadNamespaces("documentExpiration");
        expect(i18next.t(($) => $.header, { ns: "documentExpiration" })).toBe("Document Expiration");
    });

    it("interpolates without altering whitespace", async () => {
        await i18next.loadNamespaces("documentExpiration");
        expect(
            i18next.t(($) => $.licenseLimitAlert, {
                ns: "documentExpiration",
                hours: 36,
                seconds: 129600,
            })
        ).toBe("Your current license does not allow a frequency higher than 36 hours (129600 seconds)");
    });

    it("transforms output under the pseudo locale", async () => {
        await i18next.loadNamespaces("documentExpiration");
        const english = i18next.t(($) => $.header, { ns: "documentExpiration" });

        await i18next.changeLanguage(pseudoLocale);
        await i18next.loadNamespaces("documentExpiration");
        const pseudo = i18next.t(($) => $.header, { ns: "documentExpiration" });

        expect(pseudo).not.toBe(english);
        // Pseudo output stays recognisable but visibly marked and lengthened.
        expect(pseudo).toContain("[");
        expect(pseudo.length).toBeGreaterThan(english.length);
    });
});
