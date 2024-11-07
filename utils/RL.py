import json
import random
from collections import defaultdict
import math
import numpy as np
import asyncio
import aiohttp
import concurrent.futures
import time

class ConfigManager:
    def __init__(self, config_file="config.json", learning_rate=0.1, discount_factor=0.9, initial_exploration_rate=1.0, exploration_decay=0.99):
        self.config_file = config_file
        self.parameters = self.load_config()
        self.learning_rate = learning_rate
        self.discount_factor = discount_factor
        self.initial_exploration_rate = initial_exploration_rate
        self.exploration_decay = exploration_decay

        # Policy functions dictionary
        self.policy_functions = {
            "epsilon_greedy": self.epsilon_greedy_policy,
            "softmax": self.softmax_policy,
            "ucb": self.ucb_policy,
            "thompson_sampling": self.thompson_sampling_policy,
            "decay_epsilon_greedy": self.decay_epsilon_greedy_policy,
        }

    def load_config(self):
        try:
            with open(self.config_file, "r") as file:
                data = json.load(file)
                # Convert types back from strings
                for param in data.get("parameters", {}).values():
                    if "type" in param and isinstance(param["type"], str):
                        param["type"] = eval(param["type"])
                return data.get("parameters", {})
        except FileNotFoundError:
            return {}

    def save_config(self):
        # Convert types to strings for JSON compatibility
        for param in self.parameters.values():
            if "type" in param and isinstance(param["type"], type):
                param["type"] = param["type"].__name__
        with open(self.config_file, "w") as file:
            json.dump({"parameters": self.parameters}, file, indent=4)

    def initialize_parameter(self, name, type, default, min_value, max_value):
        self.parameters[name] = {
            "value": default,
            "value_default": default,
            "min_value": min_value,
            "max_value": max_value,
            "type": type.__name__,  # Store type as string
            "q_values": defaultdict(float),
            "exploration_rate": self.initial_exploration_rate,
            "total_count": 0,
            "action_counts": defaultdict(int),
        }

    def get_value(self, name, type="int", default=0, min_value=0, max_value=20, policy="epsilon_greedy"):
        if name not in self.parameters:
            self.initialize_parameter(name, type, default, min_value, max_value)

        # Retrieve the policy function or raise an error if unsupported
        policy_func = self.policy_functions.get(policy)
        if not policy_func:
            raise ValueError(f"Unsupported policy '{policy}'. Available policies are: {list(self.policy_functions.keys())}")

        # Apply the selected policy
        self.parameters[name]["value"] = policy_func(name, self.parameters[name], min_value, max_value)
        return self.parameters[name]["value"]

    def epsilon_greedy_policy(self, name, param, min_value, max_value):
        exploration_rate = param["exploration_rate"]
        q_values = param["q_values"]
        if random.random() < exploration_rate or not q_values:
            return random.randint(min_value, max_value)
        return max(q_values, key=q_values.get, default=random.randint(min_value, max_value))

    def softmax_policy(self, name, param, min_value, max_value):
        exploration_rate = param["exploration_rate"]
        q_values = param["q_values"]
        exp_values = {action: math.exp(q / exploration_rate) for action, q in q_values.items()}
        total = sum(exp_values.values())
        if total == 0:
            return random.randint(min_value, max_value)
        probabilities = {action: exp_val / total for action, exp_val in exp_values.items()}
        actions, probs = zip(*probabilities.items())
        return random.choices(actions, probs)[0]

    def ucb_policy(self, name, param, min_value, max_value):
        total_count = param["total_count"] + 1  # To avoid division by zero
        ucb_values = {}
        q_values = param["q_values"]
        action_counts = param["action_counts"]

        for action in range(min_value, max_value + 1):
            # Initialize q_values and action_counts for action if not already present
            if action not in action_counts:
                action_counts[action] = 0
            if action not in q_values:
                q_values[action] = 0

            count = action_counts[action] + 1  # To avoid division by zero
            bonus = math.sqrt((2 * math.log(total_count)) / count)
            ucb_values[action] = q_values[action] + bonus

        selected_action = max(ucb_values, key=ucb_values.get)
        param["action_counts"][selected_action] += 1
        param["total_count"] += 1
        return selected_action

    def thompson_sampling_policy(self, name, param, min_value, max_value):
        q_values = param["q_values"]
        action_counts = param["action_counts"]

        # Initialize beta parameters for each action, ensuring positive values for alpha and beta
        beta_params = {action: (1, 1) for action in range(min_value, max_value + 1)}
        for action in q_values:
            alpha = max(1, 1 + q_values[action])  # Successes, minimum 1
            beta = max(1, 1 + (action_counts.get(action, 0) - q_values[action]))  # Failures, minimum 1
            beta_params[action] = (alpha, beta)
        
        # Sample from the Beta distribution for each action
        sampled_values = {action: np.random.beta(alpha, beta) for action, (alpha, beta) in beta_params.items()}
        selected_action = max(sampled_values, key=sampled_values.get)

        # Initialize action count for selected_action if not present
        if selected_action not in action_counts:
            action_counts[selected_action] = 0
        action_counts[selected_action] += 1

        return selected_action

    def decay_epsilon_greedy_policy(self, name, param, min_value, max_value):
        param["exploration_rate"] *= self.exploration_decay
        return self.epsilon_greedy_policy(name, param, min_value, max_value)

    def step(self, parameter_names, reward):
        for name in parameter_names:
            param = self.parameters[name]
            value = param["value"]
            q_values = param["q_values"]

            # Ensure q_values has an entry for the current value
            if value not in q_values:
                q_values[value] = 0

            best_next_value = max(q_values.values(), default=0)
            old_value = q_values[value]

            # Q-learning update
            q_values[value] += self.learning_rate * (
                reward + self.discount_factor * best_next_value - old_value
            )
            # Decay exploration rate
            param["exploration_rate"] *= self.exploration_decay

    def reset(self):
        for name in self.parameters:
            self.parameters[name]["q_values"].clear()
            self.parameters[name]["exploration_rate"] = self.initial_exploration_rate
            self.parameters[name]["total_count"] = 0
            self.parameters[name]["action_counts"].clear()
        self.save_config()

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

def process_data(data):
    # Simulate data processing
    time.sleep(0.01)  # Simulate processing time

async def make_requests(max_concurrency, num_requests, url, num_workers):
    connector = aiohttp.TCPConnector(limit=max_concurrency)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        loop = asyncio.get_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_workers)
        successes = 0
        errors = 0

        for _ in range(num_requests):
            tasks.append(loop.create_task(fetch(session, url)))

        for future in asyncio.as_completed(tasks):
            try:
                data = await future
                # Process data in a worker thread
                loop.run_in_executor(executor, process_data, data)
                successes += 1
            except Exception as e:
                errors += 1

        executor.shutdown(wait=True)
        return successes, errors

# def main():
#     # Initialize ConfigManager with a configuration file path and learning parameters
#     config_manager = ConfigManager(
#         config_file="config_test.json",
#         learning_rate=0.1,
#         discount_factor=0.9,
#         initial_exploration_rate=1.0,
#         exploration_decay=0.95
#     )

#     steps = 30   # Number of steps to simulate for each policy

#     # List of policies to test
#     policies = ["epsilon_greedy", "softmax", "ucb", "thompson_sampling", "decay_epsilon_greedy"]

#     # URL to fetch data from
#     url = "https://httpbin.org/get"  # Using httpbin for testing

#     # Number of requests to make in total
#     num_requests = 100

#     # Loop through each policy and run a test
#     for policy in policies:
#         print(f"\nTesting policy: {policy}")
        
#         # Reset parameters and exploration rate for fresh start with each policy
#         config_manager.reset()
        
#         for step_num in range(steps):
#             print(f"\nStep {step_num + 1} for policy {policy}")

#             # Retrieve or initialize values for parameters 'max_concurrency' and 'num_workers' with the specified policy
#             max_concurrency = config_manager.get_value(
#                 "max_concurrency", default=10, min_value=1, max_value=50, policy=policy
#             )
#             num_workers = config_manager.get_value(
#                 "num_workers", default=5, min_value=1, max_value=20, policy=policy
#             )

#             print(f"Parameters - max_concurrency: {max_concurrency}, num_workers: {num_workers}")

#             # Start time
#             start_time = time.time()

#             # Run the asynchronous requests
#             loop = asyncio.get_event_loop()
#             successes, errors = loop.run_until_complete(
#                 make_requests(max_concurrency, num_requests, url, num_workers)
#             )

#             # End time
#             end_time = time.time()
#             duration = end_time - start_time

#             # Calculate throughput and define reward
#             throughput = successes / duration
#             error_penalty = errors * 10  # Penalty per error
#             reward = throughput - error_penalty

#             print(f"Successes: {successes}, Errors: {errors}, Duration: {duration:.2f}s, Throughput: {throughput:.2f}, Reward: {reward:.2f}")

#             # Update Q-table based on the reward
#             config_manager.step(["max_concurrency", "num_workers"], reward)

#             # Optionally, stop if reward exceeds a certain threshold
#             if reward > 100:
#                 print(f"Optimal parameters found at step {step_num + 1}!")
#                 break

#         # Save configuration after testing each policy
#         config_manager.save_config()
#         print(f"\nConfiguration saved for policy {policy}.")

# if __name__ == "__main__":
#     main()
