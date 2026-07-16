import { HrHeader } from "components/common/HrHeader";
import { AboutViewHeading } from "components/common/AboutView";
import Button from "react-bootstrap/Button";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import React from "react";
import { Icon } from "components/common/Icon";
import { MultiCheckboxToggle } from "components/common/toggles/MultiCheckboxToggle";
import IconName from "typings/server/icons";
import { useAppUrls } from "hooks/useAppUrls";
import { useNewOngoingTasks } from "components/pages/database/tasks/shared/shared";
import { EmptySet } from "components/common/EmptySet";
import { AddNewOngoingTaskAboutView } from "components/pages/database/tasks/ongoingTasks/partials/AddNewOngoingTaskAboutView";
import NavigationCard, { NavigationCardProps } from "components/common/navigationCard/NavigationCard";

interface AddNewOngoingTaskProps {
    isAiOnly: boolean;
}

export default function AddNewOngoingTask({ queryParams }: ReactQueryParamsProps<AddNewOngoingTaskProps>) {
    const isAiOnly = queryParams?.isAiOnly;

    const { forCurrentDatabase, appUrl } = useAppUrls();
    const { filteredTasks, categoryList, searchText, selectedCategories, setSearchText, setSelectedCategories } =
        useNewOngoingTasks({ isAiOnly });

    const serverWideTasksUrl = appUrl.forServerWideTasks();
    const ongoingTasksUrl = forCurrentDatabase.ongoingTasksUrl();
    const aiTasksUrl = forCurrentDatabase.aiTasks();

    return (
        <div className="content-margin">
            <div className="d-flex justify-content-between">
                <AboutViewHeading
                    title={isAiOnly ? "Add AI task" : "Add a database task"}
                    icon="tasks"
                    iconAddon="plus"
                    marginBottom={4}
                />
                {!isAiOnly && (
                    <div className="d-flex align-items-start gap-3">
                        <Button
                            size="sm"
                            target="_blank"
                            href={serverWideTasksUrl}
                            title="Go to the Server-Wide Tasks view"
                            variant="link"
                        >
                            <Icon icon="server-wide-tasks" />
                            Server-Wide Tasks
                        </Button>
                        <AddNewOngoingTaskAboutView />
                    </div>
                )}
            </div>
            <Button href={isAiOnly ? aiTasksUrl : ongoingTasksUrl} className="rounded-pill" variant="secondary">
                <Icon icon="arrow-left" />
                {isAiOnly ? "Back to AI Tasks" : "Back to ongoing tasks"}
            </Button>
            <Row className="d-flex row-gap-2 my-3">
                <Col>
                    <div className="flex-grow">
                        <div className="small-label ms-1 mb-1">Search by name</div>
                        <div className="clearable-input">
                            <Form.Control
                                type="text"
                                accessKey="/"
                                placeholder="e.g. Embeddings Generation"
                                title="Filter tasks"
                                className="filtering-input"
                                value={searchText}
                                onChange={(e) => setSearchText(e.target.value)}
                            />
                            {searchText && (
                                <div className="clear-button">
                                    <Button variant="secondary" size="sm" onClick={() => setSearchText("")}>
                                        <Icon icon="clear" margin="m-0" />
                                    </Button>
                                </div>
                            )}
                        </div>
                    </div>
                </Col>
                {!isAiOnly && (
                    <Col xs="auto">
                        <MultiCheckboxToggle
                            inputItems={categoryList}
                            label="Filter by category"
                            selectedItems={selectedCategories}
                            setSelectedItems={(x) => setSelectedCategories(x)}
                            selectAll
                            selectAllLabel="Select All"
                        />
                    </Col>
                )}
            </Row>
            <OngoingTasksList filteredTasks={filteredTasks} isAiOnly={isAiOnly} />
        </div>
    );
}

interface TaskCategory {
    categoryName: string;
    categoryIcon: IconName;
    tasks: NavigationCardProps[];
}

interface OngoingTasksListProps {
    filteredTasks: TaskCategory[];
    isAiOnly: boolean;
}

export function OngoingTasksList({ filteredTasks, isAiOnly }: OngoingTasksListProps) {
    if (filteredTasks.length === 0) {
        return <EmptySet>No tasks match your filter criteria</EmptySet>;
    }

    return (
        <>
            {filteredTasks.map((category, index) => (
                <div className="pb-2" key={index}>
                    {!isAiOnly && (
                        <HrHeader>
                            <Icon icon={category.categoryIcon} />
                            {category.categoryName}
                        </HrHeader>
                    )}
                    <div className="d-grid gap-3 navigation-cards-grid">
                        {category.tasks.map((task) => (
                            <NavigationCard key={task.title} {...task} />
                        ))}
                    </div>
                </div>
            ))}
        </>
    );
}
