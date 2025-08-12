import { CSSProperties, ReactNode, useState } from "react";
import { PopoverWithHover, PopoverWithHoverProps } from "./PopoverWithHover";
import Popover from "react-bootstrap/Popover";
import classNames from "classnames";

interface PopoverWithHoverWrapperProps extends Omit<PopoverWithHoverProps, "target"> {
    message: ReactNode | ReactNode[];
    isInPopoverBody?: boolean;
    inline?: boolean;
    targetClassname?: string;
    targetStyle?: CSSProperties;
}

export default function PopoverWithHoverWrapper({
    children,
    message,
    isInPopoverBody = true,
    targetClassname,
    targetStyle,
    ...rest
}: PopoverWithHoverWrapperProps) {
    const [target, setTarget] = useState<HTMLElement>();
    return (
        <>
            <div style={targetStyle} ref={setTarget} className={classNames(targetClassname, "d-flex align-self-center")}>
                {children}
            </div>
            {message && (
                <PopoverWithHover target={target} {...rest}>
                    {isInPopoverBody ? <Popover.Body>{message}</Popover.Body> : message}
                </PopoverWithHover>
            )}
        </>
    );
}
