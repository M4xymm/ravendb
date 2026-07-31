import React from "react";
import i18next from "i18next";
import { useTranslation } from "react-i18next";
import { rtlRender } from "test/rtlTestUtils";
import LanguageSwitcher from "./LanguageSwitcher";

describe("LanguageSwitcher", () => {
    afterEach(async () => {
        await i18next.changeLanguage("en");
    });

    it("shows the active language", async () => {
        const { screen } = rtlRender(<LanguageSwitcher />);

        expect(await screen.findByText("English")).toBeInTheDocument();
    });

    it("translates surrounding content in place when the language changes", async () => {
        // Mirrors real usage: useTranslation subscribes the component to
        // language changes, so it re-renders without a page reload.
        function Probe() {
            const { t } = useTranslation("documentExpiration");
            return (
                <>
                    <LanguageSwitcher />
                    <span>{t(($) => $.header)}</span>
                </>
            );
        }

        const { screen } = rtlRender(<Probe />);
        expect(await screen.findByText("Document Expiration")).toBeInTheDocument();

        await i18next.changeLanguage("pl");

        expect(await screen.findByText("Wygasanie dokumentów")).toBeInTheDocument();
    });
});
