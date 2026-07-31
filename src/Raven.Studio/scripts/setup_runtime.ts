import "@testing-library/jest-dom";
import { commonInit } from "components/common/shell/setup";

// commonInit loads the shared translation namespace asynchronously; awaiting it
// keeps tests from racing the first render and avoids an unhandled rejection.
beforeAll(async () => {
    await commonInit();
});
