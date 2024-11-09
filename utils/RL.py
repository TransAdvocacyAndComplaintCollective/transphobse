import json
import random
import math
import numpy as np
from collections import defaultdict
from itertools import product
from typing import List

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

    def load_config(self):
        try:
            with open(self.config_file, "r") as file:
                data = json.load(file)
                type_mapping = {"int": int, "float": float, "str": str, "bool": bool}
                for param in data.get("parameters", {}).values():
                    if "type_name" in param:
                        param["value_type"] = type_mapping.get(param["type_name"], str)
                        del param["type_name"]
                self.q_values.update({
                    param: defaultdict(float, {tuple(map(int, k.split(','))): v for k, v in values.items()})
                    for param, values in data.get("q_values", {}).items()
                })
                return data.get("parameters", {})
        except FileNotFoundError:
            return {}

    def save_config(self):
        for param in self.parameters.values():
            if "value_type" in param and isinstance(param["value_type"], type):
                param["type_name"] = param["value_type"].__name__
                del param["value_type"]
        q_values_serializable = {
            param: {','.join(map(str, k)): v for k, v in values.items()}
            for param, values in self.q_values.items()
        }
        with open(self.config_file, "w") as file:
            json.dump({"parameters": self.parameters, "q_values": q_values_serializable}, file, indent=4)

    def initialize_parameter(self, name, value_type, default, min_value, max_value, bias_init=1.0):
        self.parameters[name] = {
            "value": default,
            "value_default": default,
            "min_value": min_value,
            "max_value": max_value,
            "value_type": value_type,
            "type_name": value_type.__name__,
            "exploration_rate": self.initial_exploration_rate
        }
        self.value_types[name] = value_type
        self.value_ranges[name] = (min_value, max_value)
        self.q_values[name][(default,)] = bias_init

    def get_combined_action_space(self, parameter_names: List[str]):
        ranges = []
        for name in parameter_names:
            if name not in self.value_ranges:
                self.initialize_parameter(name, int, 100, 1, 1000)
            min_value, max_value = self.value_ranges[name]
            ranges.append(range(min_value, max_value + 1))
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

    def meta_policy(self, parameter_name):
        return max(self.policy_functions.keys(), key=lambda p: self.policy_rewards[parameter_name][p] / (self.policy_counts[parameter_name][p] + 1))

    def epsilon_greedy_policy(self, parameter_name: str):
        exploration_rate = self.exploration_rates[parameter_name]
        action_space = self.get_combined_action_space([parameter_name])
        if random.random() < exploration_rate:
            return random.choice(action_space)
        filtered_q_values = {action: self.q_values[parameter_name][action] for action in action_space if action in self.q_values[parameter_name]}
        return min(filtered_q_values, key=filtered_q_values.get)

    def decay_epsilon_greedy_policy(self, parameter_name: str):
        self.exploration_rates[parameter_name] *= self.exploration_decay
        return self.epsilon_greedy_policy(parameter_name)

    def softmax_policy(self, parameter_name: str):
        action_space = self.get_combined_action_space([parameter_name])
        exp_values = {action: math.exp(-self.q_values[parameter_name].get(action, 0)) for action in action_space}
        total = sum(exp_values.values())
        probabilities = {action: val / total for action, val in exp_values.items()}
        actions, probs = zip(*probabilities.items())
        return random.choices(actions, probs)[0]

    def ucb_policy(self, parameter_name: str):
        total_count = self.total_counts[parameter_name] + 1
        ucb_values = {}
        action_space = self.get_combined_action_space([parameter_name])
        for action in action_space:
            count = self.action_counts[parameter_name][action] + 1
            q_value = self.q_values[parameter_name].get(action, 0)
            bonus = math.sqrt((2 * math.log(total_count)) / count)
            ucb_values[action] = -q_value + bonus
        return max(ucb_values, key=ucb_values.get)

    def lcb_policy(self, parameter_name: str):
        total_count = self.total_counts[parameter_name] + 1
        lcb_values = {}
        action_space = self.get_combined_action_space([parameter_name])
        for action in action_space:
            count = self.action_counts[parameter_name][action] + 1
            q_value = self.q_values[parameter_name].get(action, 0)
            confidence_term = math.sqrt((2 * math.log(total_count)) / count)
            lcb_values[action] = q_value - confidence_term
        return max(lcb_values, key=lcb_values.get)

    def thompson_sampling_policy(self, parameter_name: str):
        action_space = self.get_combined_action_space([parameter_name])
        sampled_values = {action: np.random.beta(1 + self.q_values[parameter_name].get(action, 0), 1 + self.action_counts[parameter_name][action]) for action in action_space}
        return max(sampled_values, key=sampled_values.get)

    def step(self, parameter_names: List[str], reward):
        for parameter_name in parameter_names:
            policy = self.meta_policy(parameter_name) if self.global_policy == "adaptive" else self.global_policy
            selected_action = self.policy_functions[policy](parameter_name)
            q_values = self.q_values[parameter_name]

            if selected_action not in q_values:
                q_values[selected_action] = 0

            best_next_value = max(q_values.values(), default=0)
            old_value = q_values[selected_action]
            q_values[selected_action] += self.learning_rate * (reward + self.discount_factor * best_next_value - old_value)

            self.policy_rewards[parameter_name][policy] += reward
            self.policy_counts[parameter_name][policy] += 1
            self.exploration_rates[parameter_name] *= self.exploration_decay

    def reset(self):
        self.q_values.clear()
        self.exploration_rates = defaultdict(lambda: self.initial_exploration_rate)
        self.total_counts = defaultdict(int)
        self.action_counts.clear()
        self.policy_rewards.clear()
        self.policy_counts.clear()
        self.save_config()
