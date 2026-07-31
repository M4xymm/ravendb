export default async function loadTranslations(locale: string) {
    try {
        const translations = await import(`./i18n/_gt/${locale}.json`);
        return translations.default;
    } catch (error) {
        console.warn(`No translations found for ${locale}`);
        return {};
    }
}
