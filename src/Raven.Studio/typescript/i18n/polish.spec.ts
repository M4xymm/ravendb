import i18next from "i18next";
import commonEn from "./locales/en/common.json";
import documentExpirationEn from "./locales/en/documentExpiration.json";
import commonPl from "./locales/pl/common.json";
import documentExpirationPl from "./locales/pl/documentExpiration.json";

function flatten(obj: Record<string, unknown>, prefix = ""): string[] {
    return Object.entries(obj).flatMap(([key, value]) => {
        const path = prefix ? `${prefix}.${key}` : key;
        return value && typeof value === "object"
            ? flatten(value as Record<string, unknown>, path)
            : [path];
    });
}

describe("Polish translations", () => {
    afterEach(async () => {
        await i18next.changeLanguage("en");
    });

    it("covers every English key", () => {
        expect(flatten(commonPl).sort()).toEqual(flatten(commonEn).sort());
        expect(flatten(documentExpirationPl).sort()).toEqual(flatten(documentExpirationEn).sort());
    });

    it("renders Polish text once the language is switched", async () => {
        await i18next.changeLanguage("pl");
        await i18next.loadNamespaces(["common", "documentExpiration"]);

        expect(i18next.t(($) => $.header, { ns: "documentExpiration" })).toBe("Wygasanie dokumentów");
        expect(i18next.t(($) => $.save, { ns: "common" })).toBe("Zapisz");
    });

    it("interpolates values into Polish text", async () => {
        await i18next.changeLanguage("pl");
        await i18next.loadNamespaces("documentExpiration");

        expect(
            i18next.t(($) => $.licenseLimitAlert, {
                ns: "documentExpiration",
                hours: 36,
                seconds: 129600,
            })
        ).toBe("Twoja licencja nie pozwala na częstotliwość większą niż 36 godz. (129600 sekund)");
    });

    it("keeps the interpolation placeholders identical across locales", () => {
        const placeholders = (value: string) => (value.match(/{{\s*\w+\s*}}/g) ?? []).sort();

        for (const key of ["frequencyPlaceholder", "licenseLimitAlert"] as const) {
            expect(placeholders(documentExpirationPl[key])).toEqual(placeholders(documentExpirationEn[key]));
        }
    });

    it("keeps the Trans tag indices identical across locales", () => {
        const tags = (value: string) => (value.match(/<\/?\d+>/g) ?? []).sort();

        for (const key of ["intro", "scanItem", "expiresItem"] as const) {
            expect(tags(documentExpirationPl.aboutView[key])).toEqual(tags(documentExpirationEn.aboutView[key]));
        }
    });
});
