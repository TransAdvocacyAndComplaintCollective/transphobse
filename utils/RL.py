import json
import random
import numpy as np
import logging
from typing import Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfigManager:
    def __init__(self, config_file="config.json"):
        self.config_file = config_file
        self.q_values = {}  # Use standard dict
        self.exploration_rates = {}  # Use standard dict
        self.meta_policy_dict = {}
        self.parameters = {}
        self.last_policies_used = {}  # Keep track of policies used

        # RL Hyperparameters
        self.tolerance = 1e-3
        self.learning_rate = 0.1
        self.discount_factor = 0.9
        self.exploration_decay = 0.99
        self.initial_exploration_rate = 1.0
        self.step_size = 1  # Define step size for parameter updates

        # Define policy functions
        self.policy_functions = {
            "epsilon_greedy": self.epsilon_greedy_policy,
            "boltzmann": self.boltzmann_policy,
            "thompson_sampling": self.thompson_sampling_policy,
            "ucb": self.ucb_policy,
            "softmax": self.softmax_policy,
            "greedy": self.greedy_policy,
            "random": self.random_policy,
        }

        self.load_config()

    def random_policy(self, parameter_name: str) -> int:
        min_value = self.parameters[parameter_name]["min_value"]
        max_value = self.parameters[parameter_name]["max_value"]
        return random.randint(min_value, max_value)

    def greedy_policy(self, parameter_name: str) -> int:
        param_values = self.q_values.get(parameter_name, {})
        if not param_values:
            return self.parameters[parameter_name]["value"]

        # Find the action with the highest Q-value
        best_action = max(param_values, key=lambda a: param_values[a]['Q'])
        return best_action

    def epsilon_greedy_policy(self, parameter_name: str) -> int:
        if random.random() < self.exploration_rates.get(parameter_name, self.initial_exploration_rate):
            return self.random_policy(parameter_name)
        return self.greedy_policy(parameter_name)

    def meta_policy(self, parameter_name: str) -> str:
        # Ensure meta_policy_dict entry exists
        if parameter_name not in self.meta_policy_dict:
            self.meta_policy_dict[parameter_name] = {
                "epsilon_greedy": 0.5,
                "boltzmann": 0.5,
                "thompson_sampling": 0.5,
                "ucb": 0.5,
                "softmax": 0.5,
                "greedy": 0.5,
                "random": 0.5,
            }

        # Calculate the total score for normalization
        total_score = sum(self.meta_policy_dict[parameter_name].values())
        if total_score == 0:
            total_score = 1.0  # Prevent division by zero

        # Normalize scores and calculate the weighted score for each policy
        weighted_scores = {
            policy: (score / total_score)
            for policy, score in self.meta_policy_dict[parameter_name].items()
        }

        # Select a policy based on weighted probabilities
        policies = list(weighted_scores.keys())
        probabilities = np.array([weighted_scores[policy] for policy in policies])
        probabilities /= probabilities.sum()  # Normalize probabilities

        selected_policy = np.random.choice(policies, p=probabilities)
        return selected_policy

    def save_config(self) -> None:
        try:
            # Prepare data for saving
            data = {
                "parameters": self.parameters,
                "q_values": {
                    param: {str(action): {"Q": values['Q'], "N": values['N']}
                            for action, values in actions.items()}
                    for param, actions in self.q_values.items()
                },
                "exploration_rates": self.exploration_rates,
                "meta_policy": self.meta_policy_dict,
            }

            # Save the configuration to JSON
            with open(self.config_file, "w") as file:
                json.dump(data, file, indent=2)

            logger.info("Configuration saved successfully.")

        except Exception as e:
            logger.error(f"Error saving configuration: {e}")

    def load_config(self) -> None:
        try:
            with open(self.config_file, "r") as file:
                data = json.load(file)
                self.parameters = data.get("parameters", {})

                # Load Q-values
                for param, actions in data.get("q_values", {}).items():
                    self.q_values[param] = {}
                    for action_str, values in actions.items():
                        action = int(action_str)
                        self.q_values[param][action] = {'Q': float(values["Q"]), 'N': int(values["N"])}

                # Load exploration rates
                self.exploration_rates = {k: float(v) for k, v in data.get("exploration_rates", {}).items()}

                # Load meta_policy_dict
                self.meta_policy_dict = {
                    k: {policy: float(v) for policy, v in policies.items()}
                    for k, policies in data.get("meta_policy", {}).items()
                }

            logger.info("Configuration loaded successfully.")

        except FileNotFoundError:
            logger.warning(f"Config file '{self.config_file}' not found. Starting with empty configuration.")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")

    def initialize_q_values(self, name: str) -> None:
        # No need to pre-initialize all Q-values to save memory
        if name not in self.q_values:
            self.q_values[name] = {}
            logger.info(f"Q-values initialized for parameter '{name}'.")

    def initialize_parameter(self, name: str, value_type, default, min_value=0, max_value=10):
        if name not in self.parameters:
            self.parameters[name] = {
                "value": default,
                "min_value": min_value,
                "max_value": max_value,
                "type": value_type.__name__,
            }
            logger.info(f"Parameter '{name}' initialized with default value {default}.")

        # Initialize Q-values if not present
        self.initialize_q_values(name)

        # Ensure exploration rate is set
        self.exploration_rates.setdefault(name, self.initial_exploration_rate)
        logger.info(f"Exploration rate for '{name}' set to {self.exploration_rates[name]}.")
        return self.parameters[name]["value"]

    def set_env_value(self, name: str, value: Any, value_type: type = int) -> None:
        if name in self.parameters:
            self.parameters[name]["value"] = value_type(value)
            logger.debug(f"Parameter '{name}' set to {value_type(value)}.")
        else:
            logger.warning(f"Parameter '{name}' not initialized. Please initialize it first.")

    def get_value(self, name: str, default: Any = None, value_type: type = int) -> Any:
        param = self.parameters.get(name)

        if param is None:
            # Return the default value if the parameter is not initialized
            logger.warning(f"Parameter '{name}' not found. Returning default value: {default}")
            return default

        # Ensure the value is cast to the specified type
        return value_type(param["value"])

    def step(self, reward: float, parameter_names: list) -> None:
        """
        Update Q-values and parameter values based on the received reward.
        Args:
            reward (float): The received reward.
            parameter_names (list): The list of parameter names to update.
        """
        try:
            for param in parameter_names:
                # Ensure q_values[param] exists
                if param not in self.q_values:
                    self.q_values[param] = {}

                # Select action using a policy
                policy_name = self.meta_policy(param)
                policy = self.policy_functions.get(policy_name, self.epsilon_greedy_policy)
                action = policy(param)
                self.last_policies_used[param] = policy_name

                # Get the previous action taken
                prev_action = self.parameters[param]["value"]

                # Update Q-value for the previous action
                if prev_action not in self.q_values[param]:
                    self.q_values[param][prev_action] = {'Q': 0.0, 'N': 0}

                Q_prev = self.q_values[param][prev_action]['Q']
                N_prev = self.q_values[param][prev_action]['N']

                # Update Q-value
                alpha = self.learning_rate
                Q_prev = Q_prev + alpha * (reward - Q_prev)

                # Increment count
                N_prev += 1

                # Store back
                self.q_values[param][prev_action]['Q'] = Q_prev
                self.q_values[param][prev_action]['N'] = N_prev

                logger.info(f"Updated Q-value for parameter '{param}', action {prev_action}: Q={Q_prev}, N={N_prev}")

                # Update the meta-policy scores based on the reward
                if policy_name:
                    # Ensure meta_policy_dict entry exists
                    if param not in self.meta_policy_dict:
                        self.meta_policy_dict[param] = {
                            "epsilon_greedy": 0.5,
                            "boltzmann": 0.5,
                            "thompson_sampling": 0.5,
                            "ucb": 0.5,
                            "softmax": 0.5,
                            "greedy": 0.5,
                            "random": 0.5,
                        }
                    old_score = self.meta_policy_dict[param][policy_name]
                    new_score = old_score + alpha * (reward - old_score)
                    self.meta_policy_dict[param][policy_name] = new_score
                    logger.debug(f"Updated meta-policy score for '{param}' policy '{policy_name}': {new_score}")

                # Adjust exploration rate
                self.exploration_rates[param] *= self.exploration_decay
                self.exploration_rates[param] = max(self.exploration_rates[param], 0.1)

                # Set the new action as the parameter value
                self.set_env_value(param, action)

                logger.info(f"Parameter '{param}' set to new action {action} using policy '{policy_name}'.")

                logger.debug(f"Exploration rate for '{param}' adjusted to {self.exploration_rates[param]:.4f}")

            logger.info("Step completed with Q-values and parameter values updated.")

        except Exception as e:
            logger.error(f"Error in step(): {e}")

    # Additional policy methods can be added back if needed
    def boltzmann_policy(self, parameter_name: str, tau: float = 1.0) -> int:
        param_values = self.q_values.get(parameter_name, {})
        if not param_values:
            return self.random_policy(parameter_name)

        actions = list(param_values.keys())
        Q_values = np.array([param_values[action]['Q'] for action in actions])

        # Apply temperature scaling
        exp_values = np.exp(Q_values / tau)
        probabilities = exp_values / np.sum(exp_values)

        selected_action = np.random.choice(actions, p=probabilities)
        return selected_action

    def softmax_policy(self, parameter_name: str, temperature: float = 1.0) -> int:
        return self.boltzmann_policy(parameter_name, temperature)

    def ucb_policy(self, parameter_name: str, c: float = 2.0) -> int:
        param_values = self.q_values.get(parameter_name, {})
        if not param_values:
            return self.random_policy(parameter_name)

        total_counts = sum([values['N'] for values in param_values.values()])
        if total_counts == 0:
            return self.random_policy(parameter_name)

        ucb_values = {}
        for action, values in param_values.items():
            Q_a = values['Q']
            N_a = values['N']
            exploration_bonus = c * np.sqrt(np.log(total_counts + 1) / (N_a + 1))
            ucb_values[action] = Q_a + exploration_bonus

        best_action = max(ucb_values, key=ucb_values.get)
        return best_action

    def thompson_sampling_policy(self, parameter_name: str) -> int:
        param_values = self.q_values.get(parameter_name, {})
        if not param_values:
            return self.random_policy(parameter_name)

        sampled_values = {}
        for action, values in param_values.items():
            Q_a = values['Q']
            N_a = values['N']
            if N_a == 0:
                sampled_values[action] = random.uniform(0, 1)
            else:
                alpha = Q_a * N_a + 1
                beta = (1 - Q_a) * N_a + 1
                sampled_values[action] = np.random.beta(alpha, beta)

        best_action = max(sampled_values, key=sampled_values.get)
        return best_action


if __name__ == "__main__":
    config_manager = ConfigManager()

    # Initialize parameters once
    config_manager.initialize_parameter("param1", int, 5, min_value=0, max_value=10)
    config_manager.initialize_parameter("param2", int, 10, min_value=5, max_value=15)
    config_manager.initialize_parameter("param3", int, 15, min_value=10, max_value=20)

    target = 67
    max_iterations = 1000  # Define a maximum number of iterations to prevent infinite loops

    for iteration in range(1, max_iterations + 1):
        # Retrieve the current values of the parameters
        value1 = config_manager.get_value("param1")
        value2 = config_manager.get_value("param2")
        value3 = config_manager.get_value("param3")

        # Compute the output
        output = value1 + value2 * value3
        print(f"Iteration {iteration}: Current output: {output} (Target: {target})")

        # Calculate the reward based on the output and target
        if output == target:
            reward = 1.0  # High reward for hitting the target
            print("Target reached!")
            # Optionally break the loop if target is reached
            break
        elif output < target:
            reward = 0.5 / (abs(output - target) + 1)  # Positive reward, inversely proportional to the difference
        else:
            reward = 0.3 / (abs(output - target) + 1)  # Lower reward for overshooting

        print(f"Calculated reward: {reward:.4f}")

        # Perform an optimization step and update the parameters
        config_manager.step(reward, list(config_manager.parameters.keys()))

        # Save the configuration after each step
        config_manager.save_config()

        # Optional: Print current parameter values
        current_params = {param: config_manager.get_value(param) for param in config_manager.parameters}
        print(f"Current Parameters: {current_params}\n")
    else:
        print("Reached maximum iterations without fully converging.")
