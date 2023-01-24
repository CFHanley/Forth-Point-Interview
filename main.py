import pandas as pd
import numpy as np
from dataclasses import dataclass

@dataclass
class MachineWearObservation:
    uid : str = ""
    machine_type_updated : str = ""
    tool_wear_min : int = 0
    target : int = 0
    correct_tool_wear_observation: int = 0
    cumulative_tool_wear: int = 0
    tool_wear_since_last_failure: int = 0


def calculate_tool_wear_values(machine_performance_observations: pd.DataFrame) -> list:

    unique_machines = machine_performance_observations.machine_type_updated.unique()
    required_columns = ["uid", "machine_type_updated", "tool_wear_[min]", "timestamp", "target"]
    all_observations = machine_performance_observations.loc[:, required_columns]

    tool_wear_values = []

    for machine in unique_machines:
        machine_filtered_observations = np.array(all_observations[all_observations.machine_type_updated == machine])
        last_observation = MachineWearObservation()

        for observation in machine_filtered_observations:
            observation_object = create_object_from_observations(observation)
            observation_object.correct_tool_wear_observation = calculate_incremental_tool_wear(observation_object, last_observation)
            observation_object.tool_wear_since_last_failure = calculate_tool_wear_since_last_failure(observation_object, last_observation)
            observation_object.cumulative_tool_wear = observation_object.correct_tool_wear_observation + last_observation.cumulative_tool_wear
            last_observation = observation_object
            tool_wear_values.append(vars(observation_object))

    return tool_wear_values


def create_object_from_observations(array_with_observations:np.array) -> dataclass():

    return MachineWearObservation(
        uid=array_with_observations[0],
        machine_type_updated=array_with_observations[1],
        tool_wear_min=array_with_observations[2],
        target=array_with_observations[4])


def calculate_tool_wear_since_last_failure(current_observation: dataclass(), last_observation: dataclass()) -> int:
    if last_observation.target == 1:
        return current_observation.correct_tool_wear_observation
    else:
        return last_observation.tool_wear_since_last_failure + current_observation.correct_tool_wear_observation


def calculate_incremental_tool_wear(current_observation: dataclass(), last_observation: dataclass())-> int:

    return 0 if current_observation.tool_wear_min == 0 else current_observation.tool_wear_min - last_observation.tool_wear_min



if __name__ == '__main__':
    machine_data = pd.read_csv("sample_interview_dataset.csv")

    machines = machine_data["Machine Type"].unique()
    machine_data.fillna("missing_observations", inplace=True)
    machine_data.columns = machine_data.columns.str.lower().str.replace(" ", "_")
    machine_data["machine_type_updated"] = machine_data.loc[:, 'machine_type'].str.lower().str.replace("_", "")
    machine_data.sort_values(by=['machine_type_updated', 'timestamp'], inplace=True)

    tool_wear_observations = pd.DataFrame(data=calculate_tool_wear_values(machine_data))
    tool_wear_observations.drop(columns=["machine_type_updated", 'tool_wear_min', "target"], inplace=True)
    machine_data = machine_data.merge(tool_wear_observations, left_on="uid", right_on="uid")
    machine_data.to_csv("machine_data_with_updated_tool_wear.csv")



