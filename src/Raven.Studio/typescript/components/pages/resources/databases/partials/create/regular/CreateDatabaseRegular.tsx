﻿import React from "react";
import { Button, CloseButton, Form, ModalBody, ModalFooter } from "reactstrap";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import Steps from "components/common/steps/Steps";
import {
    CreateDatabaseRegularFormData as FormData,
    createDatabaseRegularSchema,
} from "./createDatabaseRegularValidation";
import StepBasicInfo from "./steps/CreateDatabaseRegularStepBasicInfo";
import StepEncryption from "../../../../../../common/FormEncryption";
import StepReplicationAndSharding from "./steps/CreateDatabaseRegularStepReplicationAndSharding";
import StepNodeSelection from "./steps/CreateDatabaseRegularStepNodeSelection";
import StepPath from "../shared/CreateDatabaseStepDataDirectory";
import { DevTool } from "@hookform/devtools";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { useServices } from "components/hooks/useServices";
import databasesManager from "common/shell/databasesManager";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { tryHandleSubmit } from "components/utils/common";
import QuickCreateButton from "components/pages/resources/databases/partials/create/regular/QuickCreateButton";
import { yupResolver } from "@hookform/resolvers/yup";
import {
    Control,
    FormProvider,
    FormState,
    SubmitHandler,
    UseFormSetValue,
    UseFormTrigger,
    useForm,
    useWatch,
} from "react-hook-form";
import { useSteps } from "components/common/steps/useSteps";
import { useCreateDatabaseAsyncValidation } from "components/pages/resources/databases/partials/create/shared/useCreateDatabaseAsyncValidation";
import { createDatabaseRegularDataUtils } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularDataUtils";
import {
    CreateDatabaseStep,
    createDatabaseUtils,
} from "components/pages/resources/databases/partials/create/shared/createDatabaseUtils";
import { useEventsCollector } from "components/hooks/useEventsCollector";

interface CreateDatabaseRegularProps {
    closeModal: () => void;
    changeCreateModeToBackup: () => void;
}

export default function CreateDatabaseRegular({ closeModal, changeCreateModeToBackup }: CreateDatabaseRegularProps) {
    const { databasesService } = useServices();
    const usedDatabaseNames = useAppSelector(databaseSelectors.allDatabases).map((db) => db.name);
    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);

    const form = useForm<FormData>({
        mode: "onChange",
        defaultValues: createDatabaseRegularDataUtils.getDefaultValues(allNodeTags.length),
        resolver: (data, _, options) =>
            yupResolver(createDatabaseRegularSchema)(
                data,
                {
                    usedDatabaseNames,
                    isSharded: data.replicationAndShardingStep.isSharded,
                    isManualReplication: data.replicationAndShardingStep.isManualReplication,
                    isEncrypted: data.basicInfoStep.isEncrypted,
                },
                options
            ),
    });

    const { control, handleSubmit, formState, setValue, setError, trigger } = form;
    const formValues = useWatch({
        control,
    });
    console.log("kalczur Regular errors", formState.errors); // TODO remove

    const asyncDatabaseNameValidation = useCreateDatabaseAsyncValidation(
        formValues.basicInfoStep.databaseName,
        setError
    );

    const activeSteps = getActiveStepsList(formValues, formState, asyncDatabaseNameValidation.loading);
    const { currentStep, isFirstStep, isLastStep, goToStepWithValidation, nextStepWithValidation, prevStep } = useSteps(
        activeSteps.length
    );
    const stepViews = getStepViews(control, formValues, setValue, trigger);

    const stepValidation = createDatabaseUtils.getStepValidation(
        activeSteps[currentStep].id,
        trigger,
        asyncDatabaseNameValidation,
        formValues.basicInfoStep.databaseName
    );

    const { reportEvent } = useEventsCollector();

    const onFinish: SubmitHandler<FormData> = async (formValues) => {
        return tryHandleSubmit(async () => {
            reportEvent("database", "newDatabase");

            asyncDatabaseNameValidation.execute(formValues.basicInfoStep.databaseName);
            if (!asyncDatabaseNameValidation.result) {
                return;
            }

            databasesManager.default.activateAfterCreation(formValues.basicInfoStep.databaseName);

            const dto = createDatabaseRegularDataUtils.mapToDto(formValues, allNodeTags);
            await databasesService.createDatabase(dto, formValues.replicationAndShardingStep.replicationFactor);

            closeModal();
        });
    };

    // TODO add step validation spinner

    return (
        <FormProvider {...form}>
            <Form onSubmit={handleSubmit(onFinish)}>
                <ModalBody>
                    <DevTool control={control} />
                    <div className="d-flex  mb-5">
                        <Steps
                            current={currentStep}
                            steps={activeSteps.map(createDatabaseUtils.mapToStepItem)}
                            onClick={(step) => goToStepWithValidation(step, stepValidation)}
                            className="flex-grow me-4"
                        ></Steps>
                        <CloseButton onClick={closeModal} />
                    </div>
                    {stepViews[activeSteps[currentStep].id]}
                </ModalBody>

                <hr />
                <ModalFooter>
                    {isFirstStep ? (
                        <Button
                            type="button"
                            onClick={changeCreateModeToBackup}
                            className="rounded-pill"
                            disabled={formState.isSubmitting}
                        >
                            <Icon icon="database" addon="arrow-up" /> Create from backup
                        </Button>
                    ) : (
                        <Button type="button" onClick={prevStep} className="rounded-pill">
                            <Icon icon="arrow-thin-left" /> Back
                        </Button>
                    )}
                    <FlexGrow />
                    {!isLastStep && <QuickCreateButton formValues={formValues} isSubmitting={formState.isSubmitting} />}
                    {isLastStep ? (
                        <ButtonWithSpinner
                            type="submit"
                            color="success"
                            className="rounded-pill"
                            icon="rocket"
                            isSpinning={formState.isSubmitting}
                        >
                            Finish
                        </ButtonWithSpinner>
                    ) : (
                        <Button
                            type="button"
                            color="primary"
                            className="rounded-pill"
                            onClick={() => nextStepWithValidation(stepValidation)}
                            disabled={asyncDatabaseNameValidation.loading}
                        >
                            Next <Icon icon="arrow-thin-right" margin="ms-1" />
                        </Button>
                    )}
                </ModalFooter>
            </Form>
        </FormProvider>
    );
}

type Step = CreateDatabaseStep<FormData>;

function getActiveStepsList(
    formValues: FormData,
    formState: FormState<FormData>,
    isValidatingDatabaseName: boolean
): Step[] {
    const steps: Step[] = [
        {
            id: "basicInfoStep",
            label: "Name",
            active: true,
            isInvalid: !!formState.errors.basicInfoStep,
            isLoading: isValidatingDatabaseName,
        },
        {
            id: "encryptionStep",
            label: "Encryption",
            active: formValues.basicInfoStep.isEncrypted,
            isInvalid: !!formState.errors.encryptionStep,
        },
        {
            id: "replicationAndShardingStep",
            label: "Replication & Sharding",
            active: true,
            isInvalid: !!formState.errors.replicationAndShardingStep,
        },
        {
            id: "manualNodeSelectionStep",
            label: "Manual Node Selection",
            active: formValues.replicationAndShardingStep.isManualReplication,
            isInvalid: !!formState.errors.manualNodeSelectionStep,
        },
        {
            id: "dataDirectoryStep",
            label: "Paths Configuration",
            active: true,
            isInvalid: !!formState.errors.dataDirectoryStep,
        },
    ];

    return steps.filter((step) => step.active);
}

function getStepViews(
    control: Control<FormData>,
    formValues: FormData,
    setValue: UseFormSetValue<FormData>,
    trigger: UseFormTrigger<FormData>
): Record<keyof FormData, JSX.Element> {
    const { encryptionKeyFileName, encryptionKeyText } = createDatabaseUtils.getEncryptionData(
        formValues.basicInfoStep.databaseName,
        formValues.encryptionStep.key
    );

    return {
        basicInfoStep: <StepBasicInfo />,
        encryptionStep: (
            <StepEncryption
                control={control}
                encryptionKey={formValues.encryptionStep.key}
                fileName={encryptionKeyFileName}
                keyText={encryptionKeyText}
                setEncryptionKey={(x) => setValue("encryptionStep.key", x)}
                triggerEncryptionKey={() => trigger("encryptionStep.key")}
                encryptionKeyFieldName="encryptionStep.key"
                isSavedFieldName="encryptionStep.isKeySaved"
            />
        ),
        replicationAndShardingStep: <StepReplicationAndSharding />,
        manualNodeSelectionStep: <StepNodeSelection />,
        dataDirectoryStep: (
            <StepPath
                isBackupFolder={false}
                manualSelectedNodes={
                    formValues.replicationAndShardingStep.isManualReplication
                        ? formValues.manualNodeSelectionStep.nodes
                        : null
                }
            />
        ),
    };
}
