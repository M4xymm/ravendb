import React from "react";
import { render, act } from "@testing-library/react";
import { T, useSetLocale, useLocaleSelector } from "gt-react";

// GT's config is a process-wide singleton, already initialized by commonInit()
// via scripts/setup_runtime.ts. A second initializeGTSPA() call is ignored,
// so these tests assert against the locales declared in gt.config.json.
describe("gt-react locale switching", () => {
    it("exposes the current locale and the configured list", () => {
        let seen: { locale: string; locales: string[] };

        function Probe(): React.ReactElement | null {
            const { locale, locales } = useLocaleSelector();
            seen = { locale, locales };
            return null;
        }

        render(<Probe />);

        expect(seen.locale).toBe("en");
        expect(seen.locales).toEqual(expect.arrayContaining(["en", "pl"]));
    });

    it("persists the chosen locale to a cookie via useSetLocale", async () => {
        let setLocale: (locale: string) => void;

        function Probe(): React.ReactElement | null {
            setLocale = useSetLocale();
            return <T>Save</T>;
        }

        render(<Probe />);

        await act(async () => {
            setLocale("pl");
        });

        // setLocale writes the choice to a cookie; the rendered locale only
        // changes after the page reloads (GT calls window.location.reload(),
        // which jsdom does not implement).
        expect(document.cookie).toContain("generaltranslation.locale=pl");
    });
});
