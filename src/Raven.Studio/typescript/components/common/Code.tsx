import React, { ReactNode, useMemo } from "react";
import Prism from "prismjs";
import "./Code.scss";
import copyToClipboard from "common/copyToClipboard";
import { Icon } from "components/common/Icon";
import { Button } from "reactstrap";
import classNames from "classnames";

require("prismjs/components/prism-javascript");
require("prismjs/components/prism-csharp");
require("prismjs/components/prism-json");

type Language =
    | "plaintext"
    | "markup"
    | "html"
    | "mathml"
    | "svg"
    | "xml"
    | "ssml"
    | "atom"
    | "rss"
    | "css"
    | "clike"
    | "javascript"
    | "csharp"
    | "json";

interface CodeProps {
    code: string;
    language: Language;
    className?: string;
    hasCopyToClipboard?: boolean;
    elementToCopy?: string;
}

export default function Code({ code, language, className, hasCopyToClipboard, elementToCopy }: CodeProps) {
    const html = useMemo(() => Prism.highlight(code, Prism.languages[language], language), [code, language]);

    return (
        <div className={classNames("code d-flex flex-grow-1 position-relative", className)}>
            {hasCopyToClipboard && (
                <Button
                    className="rounded-pill position-absolute end-gutter-xs top-gutter-xs"
                    size="xs"
                    title="Copy to clipboard"
                    onClick={() => copyToClipboard.copy(`${elementToCopy}`, `Copied to clipboard`)}
                >
                    <Icon icon="copy" margin="m-0" />
                </Button>
            )}
            <pre className="code-classes d-flex flex-grow-1">
                <code className={`language-${language}`}>
                    <div dangerouslySetInnerHTML={{ __html: html }} />
                </code>
            </pre>
        </div>
    );
}
