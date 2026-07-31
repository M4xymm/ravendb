import "@testing-library/jest-dom";
import { commonInit } from "components/common/shell/setup";

// commonInit is async (it loads translations); tests render synchronously and
// fall back to the source language, so await it only to avoid an unhandled
// rejection.
beforeAll(async () => {
    await commonInit();
});
