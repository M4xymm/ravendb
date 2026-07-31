import React, { useEffect, useMemo } from "react";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { FormInput, FormSwitch } from "components/common/Form";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { tryHandleSubmit } from "components/utils/common";
import { DocumentExpirationFormData, documentExpirationYupResolver } from "./DocumentExpirationValidation";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import messagePublisher from "common/messagePublisher";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import Code from "components/common/Code";
import { useAsyncCallback } from "react-async-hook";
import ServerExpirationConfiguration = Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration;
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import moment from "moment";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import RichAlert from "components/common/RichAlert";
import { Trans, useTranslation } from "react-i18next";
import LanguageSwitcher from "components/common/LanguageSwitcher";

export const defaultItemsToProcess = 65536;

export default function DocumentExpiration() {
    const { t } = useTranslation("documentExpiration");
    const { t: tCommon } = useTranslation("common");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();

    const asyncGetExpirationConfiguration = useAsyncCallback<DocumentExpirationFormData>(async () =>
        mapToFormData(await databasesService.getExpirationConfiguration(databaseName))
    );

    const { handleSubmit, control, formState, reset, setValue, watch } = useForm<DocumentExpirationFormData>({
        resolver: documentExpirationYupResolver,
        mode: "all",
        defaultValues: asyncGetExpirationConfiguration.execute,
    });

    useDirtyFlag(formState.isDirty);
    const formValues = useWatch({ control: control });

    const { reportEvent } = useEventsCollector();

    const documentExpirationDocsLink = useRavenLink({ hash: "XBFEKZ" });

    const minPeriodForExpirationInHours = useAppSelector(licenseSelectors.statusValue("MinPeriodForExpirationInHours"));

    const defaultFeatureAvailability: FeatureAvailabilityData[] = useMemo(
        () => [
            {
                featureName: t(($) => $.minCheckFrequency),
                featureIcon: "clock",
                community: { value: 36 },
                professional: { value: Infinity },
                enterprise: { value: Infinity },
            },
        ],
        [t]
    );

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: minPeriodForExpirationInHours,
            },
        ],
    });

    const deleteFrequencyInHours = moment.duration(formValues.deleteFrequency, "seconds").asHours();

    const isLimitWarningVisible =
        minPeriodForExpirationInHours > 0 &&
        formValues.isDeleteFrequencyEnabled &&
        deleteFrequencyInHours < minPeriodForExpirationInHours;

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            switch (name) {
                case "isDocumentExpirationEnabled": {
                    if (values.isDocumentExpirationEnabled) {
                        setValue("isLimitMaxItemsToProcessEnabled", true, { shouldValidate: true });
                    } else {
                        setValue("isLimitMaxItemsToProcessEnabled", false, { shouldValidate: true });
                        setValue("isDeleteFrequencyEnabled", false, { shouldValidate: true });
                    }
                    break;
                }
                case "isLimitMaxItemsToProcessEnabled": {
                    if (values.isLimitMaxItemsToProcessEnabled) {
                        setValue("maxItemsToProcess", defaultItemsToProcess, { shouldValidate: true });
                    } else {
                        setValue("maxItemsToProcess", null, { shouldValidate: true });
                    }
                    break;
                }
                case "isDeleteFrequencyEnabled": {
                    if (!values.isDeleteFrequencyEnabled) {
                        setValue("deleteFrequency", null, { shouldValidate: true });
                    }
                    break;
                }
            }
        });
        return () => unsubscribe();
    }, [setValue, watch]);

    const onSave: SubmitHandler<DocumentExpirationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("expiration-configuration", "save");

            await databasesService.saveExpirationConfiguration(databaseName, {
                Disabled: !formData.isDocumentExpirationEnabled,
                DeleteFrequencyInSec: formData.isDeleteFrequencyEnabled ? formData.deleteFrequency : null,
                MaxItemsToProcess: formData.isLimitMaxItemsToProcessEnabled ? formData.maxItemsToProcess : null,
            });

            messagePublisher.reportSuccess(t(($) => $.saveSuccess));
            activeDatabaseTracker.default.database().hasExpirationConfiguration(formData.isDocumentExpirationEnabled);

            reset(formData);
        });
    };

    if (
        asyncGetExpirationConfiguration.status === "not-requested" ||
        asyncGetExpirationConfiguration.status === "loading"
    ) {
        return <LoadingView />;
    }

    if (asyncGetExpirationConfiguration.status === "error") {
        return (
            <LoadError error={t(($) => $.loadError)} refresh={asyncGetExpirationConfiguration.execute} />
        );
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                            <AboutViewHeading title={t(($) => $.header)} icon="document-expiration" />
                            <LanguageSwitcher className="mb-3" />
                            <ButtonWithSpinner
                                type="submit"
                                variant="primary"
                                className="mb-3"
                                icon="save"
                                disabled={!formState.isDirty || isLimitWarningVisible}
                                isSpinning={formState.isSubmitting}
                            >
                                {tCommon(($) => $.save)}
                            </ButtonWithSpinner>
                            <Col>
                                <Card>
                                    <Card.Body>
                                        <div className="vstack gap-2">
                                            <FormSwitch name="isDocumentExpirationEnabled" control={control}>
                                                {t(($) => $.enableSwitch)}
                                            </FormSwitch>
                                            <div>
                                                <FormSwitch
                                                    name="isDeleteFrequencyEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                    disabled={
                                                        formState.isSubmitting ||
                                                        !formValues.isDocumentExpirationEnabled
                                                    }
                                                >
                                                    {t(($) => $.customFrequencySwitch)}
                                                </FormSwitch>
                                                <FormInput
                                                    name="deleteFrequency"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting || !formValues.isDeleteFrequencyEnabled
                                                    }
                                                    placeholder={t(($) => $.frequencyPlaceholder, {
                                                        seconds:
                                                            minPeriodForExpirationInHours > 0
                                                                ? moment
                                                                      .duration(minPeriodForExpirationInHours, "hours")
                                                                      .asSeconds()
                                                                : 60,
                                                    })}
                                                    addon={tCommon(($) => $.seconds)}
                                                />
                                                {isLimitWarningVisible && (
                                                    <RichAlert variant="warning" className="mt-3">
                                                        {t(($) => $.licenseLimitAlert, {
                                                            hours: minPeriodForExpirationInHours,
                                                            seconds: moment
                                                                .duration(minPeriodForExpirationInHours, "hours")
                                                                .asSeconds(),
                                                        })}
                                                    </RichAlert>
                                                )}
                                            </div>
                                            <div>
                                                <FormSwitch
                                                    name="isLimitMaxItemsToProcessEnabled"
                                                    control={control}
                                                    className="mb-3"
                                                    disabled={
                                                        formState.isSubmitting ||
                                                        !formValues.isDocumentExpirationEnabled
                                                    }
                                                >
                                                    {t(($) => $.maxItemsSwitch)}
                                                </FormSwitch>
                                                <FormInput
                                                    name="maxItemsToProcess"
                                                    control={control}
                                                    type="number"
                                                    disabled={
                                                        formState.isSubmitting ||
                                                        !formValues.isLimitMaxItemsToProcessEnabled
                                                    }
                                                    addon={tCommon(($) => $.items)}
                                                />
                                            </div>
                                        </div>
                                    </Card.Body>
                                </Card>
                            </Col>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper targetId="1" icon="about" color="info">
                                <p>
                                    <Trans
                                        t={t}
                                        i18nKey={($) => $.aboutView.intro}
                                        components={{ 1: <strong /> }}
                                    />
                                </p>
                                <ul>
                                    <li>
                                        <Trans
                                            t={t}
                                            i18nKey={($) => $.aboutView.scanItem}
                                            components={{ 1: <strong /> }}
                                        />
                                    </li>
                                    <li>
                                        <Trans
                                            t={t}
                                            i18nKey={($) => $.aboutView.expiresItem}
                                            components={{ 1: <code /> }}
                                        />
                                    </li>
                                </ul>

                                <p>{t(($) => $.aboutView.exampleLabel)}</p>
                                <Code code={codeExample} language="javascript" />
                                <hr />
                                <div className="small-label mb-2">{tCommon(($) => $.usefulLinks)}</div>
                                <a href={documentExpirationDocsLink} target="_blank">
                                    <Icon icon="newtab" /> {t(($) => $.docsLink)}
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={!minPeriodForExpirationInHours}
                                data={featureAvailability}
                                isOpenedByDefault={false}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

function mapToFormData(dto: ServerExpirationConfiguration): DocumentExpirationFormData {
    if (!dto) {
        return {
            isDocumentExpirationEnabled: false,
            isDeleteFrequencyEnabled: false,
            deleteFrequency: null,
            isLimitMaxItemsToProcessEnabled: false,
            maxItemsToProcess: null,
        };
    }

    return {
        isDocumentExpirationEnabled: !dto.Disabled,
        isDeleteFrequencyEnabled: dto.DeleteFrequencyInSec != null,
        deleteFrequency: dto.DeleteFrequencyInSec,
        isLimitMaxItemsToProcessEnabled: dto.MaxItemsToProcess != null,
        maxItemsToProcess: dto.MaxItemsToProcess,
    };
}

const codeExample = `{
    "Example": 
      "Set a timestamp in the @expires metadata property",
    "@metadata": {
      "@collection": "Foo",
      "@expires": "${moment().add(1, "year").toISOString()}"
    }
}`;

