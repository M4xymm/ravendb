import React from "react";
import { useTranslation } from "react-i18next";
import Select, { SelectOption } from "components/common/select/Select";
import { supportedLocales } from "i18n/config";

interface LanguageSwitcherProps {
    className?: string;
}

const options: SelectOption[] = supportedLocales.map((locale) => ({
    value: locale.code,
    label: locale.label,
}));

/**
 * Switches the Studio's React UI language. Knockout views are unaffected.
 *
 * react-i18next re-renders subscribed components in place, so no page reload
 * is needed and unsaved form state survives the switch.
 */
export default function LanguageSwitcher({ className }: LanguageSwitcherProps) {
    const { i18n } = useTranslation();

    const selected = options.find((option) => option.value === i18n.resolvedLanguage) ?? options[0];

    return (
        <div className={className} style={{ maxWidth: "220px" }}>
            <Select
                options={options}
                value={selected}
                onChange={(option: SelectOption) => i18n.changeLanguage(option.value)}
                aria-label="Language"
            />
        </div>
    );
}
