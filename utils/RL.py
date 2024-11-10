from asyncio.log import logger
import json
import random
import math
import numpy as np
from collections import defaultdict
from itertools import product
from typing import List
from numba import jit
class ConfigManager:
    def __init__(self, config_file="config.json", learning_rate=0.1, discount_factor=0.9, initial_exploration_rate=1.0, exploration_decay=0.99, global_policy="adaptive"):
        self.config_file = config_file
        self.learning_rate = learning_rate
        self.discount_factor = discount_factor
        self.initial_exploration_rate = initial_exploration_rate
        self.exploration_decay = exploration_decay
        self.global_policy = global_policy

        # Initialize attributes before loading configuration
        self.q_values = defaultdict(lambda: defaultdict(float))
        self.exploration_rates = defaultdict(lambda: self.initial_exploration_rate)
        self.total_counts = defaultdict(int)
        self.action_counts = defaultdict(lambda: defaultdict(int))
        self.policy_variances = defaultdict(lambda: defaultdict(float))
        self.momentum = 0.9  # Set the momentum factor (you can adjust this value)
        self.momentum_values = defaultdict(lambda: defaultdict(float))
        self.value_types = {}
        self.value_ranges = {}

        # Policy functions dictionary
        self.policy_functions = {
            "epsilon_greedy": self.epsilon_greedy_policy,
            "softmax": self.softmax_policy,
            "ucb": self.ucb_policy,
            "thompson_sampling": self.thompson_sampling_policy,
            "decay_epsilon_greedy": self.decay_epsilon_greedy_policy,
            "lcb": self.lcb_policy,
        }

        # Track policy performance for adaptive switching
        self.policy_rewards = defaultdict(lambda: defaultdict(float))
        self.policy_counts = defaultdict(lambda: defaultdict(int))

        # Load existing parameters or initialize
        self.parameters = self.load_config()
        

    def save_config(self):
        """
        Save the current configuration, Q-values, and adaptive data to a JSON file.
        """
        try:
            # Convert tuple keys to strings for JSON serialization
            q_values_serializable = {
                param: {json.dumps(list(k)): v for k, v in values.items()}
                for param, values in self.q_values.items()
            }
            momentum_values_serializable = {
                param: {json.dumps(list(k)): v for k, v in values.items()}
                for param, values in self.momentum_values.items()
            }

            # Convert 'value_type' to a string for JSON serialization
            parameters_serializable = {
                param: {
                    **details,
                    "value_type": details["value_type"] if isinstance(details["value_type"], str)
                    else details["value_type"].__name__
                }
                for param, details in self.parameters.items()
            }

            # Prepare other adaptive data for serialization
            data = {
                "parameters": parameters_serializable,
                "q_values": q_values_serializable,
                "momentum_values": momentum_values_serializable,
                "exploration_rates": dict(self.exploration_rates),
                "total_counts": dict(self.total_counts),
                "action_counts": {
                    param: {json.dumps(list(action)): count for action, count in actions.items()}
                    for param, actions in self.action_counts.items()
                },
                "policy_rewards": {
                    param: dict(rewards) for param, rewards in self.policy_rewards.items()
                },
                "policy_counts": {
                    param: dict(counts) for param, counts in self.policy_counts.items()
                },
                "policy_variances": {
                    param: dict(variances) for param, variances in self.policy_variances.items()
                },
            }

            # Write data to the configuration file
            with open(self.config_file, "w") as file:
                json.dump(data, file, indent=4)

            print(f"Configuration successfully saved to '{self.config_file}'.")

        except TypeError as e:
            print(f"JSON serialization error: {e}. Please check the data types.")
        except Exception as e:
            print(f"Error saving configuration: {e}")



    def load_config(self):
        """
        Load the configuration, Q-values, and adaptive data from a JSON file.
        """
        try:
            with open(self.config_file, "r") as file:
                data = json.load(file)

                # Ensure parameters are initialized
                self.parameters = data.get("parameters", {})

                # Deserialize Q-values (convert JSON string keys back to tuples)
                self.q_values = {
                    param: defaultdict(float, {tuple(json.loads(k)): v for k, v in values.items()})
                    for param, values in data.get("q_values", {}).items()
                }
                self.momentum_values = {
                    param: defaultdict(float, {tuple(json.loads(k)): v for k, v in values.items()})
                    for param, values in data.get("momentum_values", {}).items()
                }

                # Load other adaptive data
                self.exploration_rates.update(data.get("exploration_rates", {}))
                self.total_counts.update(data.get("total_counts", {}))
                self.action_counts = {
                    param: defaultdict(int, {tuple(json.loads(k)): v for k, v in actions.items()})
                    for param, actions in data.get("action_counts", {}).items()
                }
                self.policy_rewards = {
                    param: defaultdict(float, rewards) for param, rewards in data.get("policy_rewards", {}).items()
                }
                self.policy_counts = {
                    param: defaultdict(int, counts) for param, counts in data.get("policy_counts", {}).items()
                }
                self.policy_variances = {
                    param: defaultdict(float, variances) for param, variances in data.get("policy_variances", {}).items()
                }

                # Convert 'value_type' back from string to Python type
                for param, details in self.parameters.items():
                    type_name = details.get("value_type", "int")
                    if type_name == "int":
                        self.value_types[param] = int
                    elif type_name == "float":
                        self.value_types[param] = float
                    elif type_name == "str":
                        self.value_types[param] = str
                    else:
                        self.value_types[param] = int  # Default to int if unknown

                print(f"Configuration successfully loaded from '{self.config_file}'.")
                return self.parameters

        except FileNotFoundError:
            print(f"Config file '{self.config_file}' not found. Initializing with default values.")
            self.parameters = {}
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}. Resetting to default configuration.")
            self.parameters = {}
        except Exception as e:
            print(f"Unexpected error while loading configuration: {e}. Resetting to default values.")
            self.parameters = {}

        return self.parameters





    def initialize_parameter(self, name, value_type, default, min_value, max_value, bias_init=math.ulp(0.0)):
        """
        Initialize a parameter with specified attributes and default values.
        """
        # Initialize parameter details
        self.parameters[name] = {
            "value": default,
            "value_default": default,
            "min_value": min_value,
            "max_value": max_value,
            "value_type": value_type.__name__,  # Store the type as a string
            "type_name": value_type.__name__,
            "exploration_rate": self.initial_exploration_rate,
        }

        # Set value types and ranges
        self.value_types[name] = value_type
        self.value_ranges[name] = (min_value, max_value)

        # Initialize Q-values with bias
        self.q_values[name][(default,)] = bias_init

        # Initialize adaptive data
        self.exploration_rates[name] = self.initial_exploration_rate
        self.total_counts[name] = 0
        self.action_counts[name] = defaultdict(int)
        self.policy_rewards[name] = defaultdict(float)
        self.policy_counts[name] = defaultdict(int)

        # Initialize action counts for the default action
        self.action_counts[name][(default,)] = 0

        # Debug output for initialization
        print(f"Initialized parameter '{name}' with default value {default}, "
              f"range ({min_value}, {max_value}), type '{value_type.__name__}'")



    def get_combined_action_space(self, parameter_names: List[str], max_samples: int = 100):
        ranges = []
        for name in parameter_names:
            if name not in self.value_ranges:
                self.initialize_parameter(name, int, 100, 1, 1000)
            min_value, max_value = self.value_ranges[name]
            range_size = max_value - min_value
            step_size = max(1, range_size // max_samples)
            action_space = list(range(min_value, max_value + 1, step_size))
            ranges.append(action_space)
        return list(product(*ranges))


    def get_value(self, name, default=0, value_type=int, max_value=20, min_value=1, policy="adaptive"):
        if name not in self.parameters:
            self.initialize_parameter(name, value_type, default, min_value, max_value)

        if policy == "adaptive":
            policy = self.meta_policy(name)
        policy_func = self.policy_functions.get(policy)
        if not policy_func:
            raise ValueError(f"Unsupported policy '{policy}'")

        value = policy_func(name)[0]
        self.parameters[name]["value"] = value_type(value)
        return self.parameters[name]["value"]

    def meta_policy(self, parameter_name: str):
        total_count = sum(self.policy_counts[parameter_name].values()) + 1
        best_policy = None
        best_score = float('inf')  # Initialize with infinity for minimization

        for policy in self.policy_functions:
            count = self.policy_counts[parameter_name][policy] + 1
            avg_reward = self.policy_rewards[parameter_name][policy] / count
            variance = self.policy_variances[parameter_name][policy]
            exploration_term = math.sqrt((2 * math.log(total_count)) / count)
            tuned_term = math.sqrt((variance + exploration_term) / count)
            score = avg_reward - tuned_term  # Invert for minimization

            if score < best_score:  # Minimize instead of maximize
                best_score = score
                best_policy = policy

        return best_policy

    def epsilon_greedy_policy(self, parameter_name: str):
        exploration_rate = self.exploration_rates[parameter_name]
        action_space = self.get_combined_action_space([parameter_name])

        # Initialize Q-values for actions if not already done
        for action in action_space:
            if action not in self.q_values[parameter_name]:
                self.q_values[parameter_name][action] = 0.0

        # Exploration vs. exploitation decision
        if random.random() < exploration_rate:
            return random.choice(action_space)

        # Filtered Q-values for exploitation (minimization)
        filtered_q_values = {action: self.q_values[parameter_name][action] for action in action_space}

        # Fallback to random action if no Q-values are available
        if not filtered_q_values:
            print(f"Warning: No Q-values found for '{parameter_name}'. Falling back to random choice.")
            return random.choice(action_space)

        # Select the action with the maximum Q-value
        return max(filtered_q_values, key=filtered_q_values.get)
    
    def decay_epsilon_greedy_policy(self, parameter_name: str):
        self.exploration_rates[parameter_name] *= self.exploration_decay
        return self.epsilon_greedy_policy(parameter_name)

    def softmax_policy(self, parameter_name: str):
        action_space = self.get_combined_action_space([parameter_name])
        q_values = np.array([self.q_values[parameter_name].get(action, 0) for action in action_space])
        exp_values = np.exp(q_values - np.max(q_values))  # Subtract max for numerical stability
        probabilities = exp_values / np.sum(exp_values)
        return random.choices(action_space, probabilities)[0]



    def ucb_policy(self, parameter_name: str):
        total_count = self.total_counts[parameter_name] + 1
        ucb_values = {}
        action_space = self.get_combined_action_space([parameter_name])
        for action in action_space:
            count = self.action_counts[parameter_name][action] + 1
            q_value = self.q_values[parameter_name].get(action, 0)
            bonus = math.sqrt((2 * math.log(total_count)) / count)
            ucb_values[action] = q_value + bonus  # Minimize by not inverting Q-value
        return min(ucb_values, key=ucb_values.get)  # Select the action with the smallest value
    
    def lcb_policy(self, parameter_name: str):
        total_count = self.total_counts[parameter_name] + 1
        lcb_values = {}
        action_space = self.get_combined_action_space([parameter_name])
        for action in action_space:
            count = self.action_counts[parameter_name][action] + 1
            q_value = self.q_values[parameter_name].get(action, 0)
            confidence_term = math.sqrt((2 * math.log(total_count)) / count)
            lcb_values[action] = q_value + confidence_term  # Minimize by not subtracting confidence
        return min(lcb_values, key=lcb_values.get)  # Select the action with the lowest value


    def thompson_sampling_policy(self, parameter_name: str):
        action_space = self.get_combined_action_space([parameter_name])
        # Use the inverse of Beta distribution sampling for minimization
        sampled_values = {action: 1 / np.random.beta(1 + self.q_values[parameter_name].get(action, 0), 1 + self.action_counts[parameter_name][action]) for action in action_space}
        return min(sampled_values, key=sampled_values.get)

    def update_variance(self, parameter_name: str, policy: str, reward: float):
        count = self.policy_counts[parameter_name][policy] + 1
        avg_reward = self.policy_rewards[parameter_name][policy] / count
        variance = self.policy_variances[parameter_name][policy]
        new_variance = variance + ((reward - avg_reward) ** 2 - variance) / count
        self.policy_variances[parameter_name][policy] = new_variance
        
        
    def fine_tune(self, parameter_name: str):
        """
        Perform fine-tuning by adjusting the exploration rate, reducing the learning rate,
        and using a finer search space for the given parameter.
        """
        print(f"Fine-tuning parameter '{parameter_name}'...")
    
        # Increase exploration rate temporarily for fine-tuning
        original_exploration_rate = self.exploration_rates[parameter_name]
        self.exploration_rates[parameter_name] = min(0.5, original_exploration_rate * 1.5)
    
        # Reduce learning rate for finer adjustments
        original_learning_rate = self.learning_rate
        self.learning_rate *= 0.5
    
        # Use `get_combined_action_space` with a smaller `max_samples` for finer search
        action_space = self.get_combined_action_space([parameter_name], max_samples=20)
    
        # Evaluate actions in the finer search space
        best_action = None
        best_q_value = float('inf')  # Minimize the Q-value
    
        for action in action_space:
            q_value = self.q_values[parameter_name].get((action,), 0)
            if q_value < best_q_value:
                best_q_value = q_value
                best_action = action
    
        # Update the parameter value based on the best action found
        self.parameters[parameter_name]["value"] = best_action
    
        # Restore the original exploration rate and learning rate
        self.exploration_rates[parameter_name] = original_exploration_rate
        self.learning_rate = original_learning_rate
    
        print(f"Fine-tuning complete. Selected value: {best_action} for parameter '{parameter_name}'")


    def adjust_exploration_rate(self, parameter_name: str):
        """
        Dynamically adjust the exploration rate based on reward variance and stagnation.
        """
        # Calculate the variance of the rewards for this parameter
        total_count = sum(self.policy_counts[parameter_name].values()) + 1
        avg_reward = sum(self.policy_rewards[parameter_name].values()) / total_count
        reward_variance = np.var(list(self.policy_rewards[parameter_name].values()))
    
        # Check for stagnation: if variance is low, increase exploration rate
        if reward_variance < 0.01:  # Stagnation threshold (can be tuned)
            self.exploration_rates[parameter_name] = min(
                0.5, self.exploration_rates[parameter_name] * 1.2
            )
        else:
            # Regular decay of exploration rate
            self.exploration_rates[parameter_name] *= self.exploration_decay
    
        # Ensure exploration rate does not fall below a minimum value
        self.exploration_rates[parameter_name] = max(0.01, self.exploration_rates[parameter_name])


    def step(self, parameter_names: List[str], reward: float):
        """
        Update Q-values for the given parameters based on the received reward.
        Uses momentum for faster convergence, variance tracking, and fine-tuning for adaptive policy selection.

        Args:
            parameter_names (List[str]): List of parameter names to update.
            reward (float): The reward received for the current action.
        """
        for parameter_name in parameter_names:
            # Adjust exploration rate adaptively
            self.adjust_exploration_rate(parameter_name)

            # Detect stagnation (low reward variance) and trigger fine-tuning
            reward_variance = np.var(list(self.policy_rewards[parameter_name].values()))
            if reward_variance < 0.01:  # Stagnation threshold (can be tuned)
                print(f"Stagnation detected for '{parameter_name}'. Initiating fine-tuning...")
                self.fine_tune(parameter_name)

            # Select the policy to use based on the meta-policy or global policy
            policy = self.meta_policy(parameter_name) if self.global_policy == "adaptive" else self.global_policy
            selected_action = self.policy_functions[policy](parameter_name)

            # Access Q-values for the current parameter
            q_values = self.q_values[parameter_name]
            if selected_action not in q_values:
                q_values[selected_action] = 0

            # Calculate Q-learning update with momentum
            best_next_value = max(q_values.values(), default=0)
            old_value = q_values[selected_action]
            momentum_update = self.momentum * self.momentum_values[parameter_name][selected_action]
            q_update = self.learning_rate * (reward + self.discount_factor * best_next_value - old_value)
            q_values[selected_action] += q_update + momentum_update

            # Update momentum values for the action
            self.momentum_values[parameter_name][selected_action] = q_update

            # Update policy performance data
            self.policy_rewards[parameter_name][policy] += reward
            self.policy_counts[parameter_name][policy] += 1
            self.update_variance(parameter_name, policy, reward)



    def reset(self):
        self.q_values.clear()
        self.exploration_rates = defaultdict(lambda: self.initial_exploration_rate)
        self.total_counts = defaultdict(int)
        self.action_counts.clear()
        
        self.policy_rewards.clear()
        self.policy_counts.clear()
        self.save_config()
