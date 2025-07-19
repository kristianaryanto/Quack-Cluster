# Contributing to Quack-Cluster

First off, thank you for considering contributing to Quack-Cluster\! We're excited to have you join our community. Your help is essential for keeping this project great. Every contribution, from bug reports to new features, is valuable.

This document provides guidelines for contributing to the project.

-----

## 🤝 How to Contribute

There are many ways to contribute, including:

  * **Reporting Bugs**: If you find a bug, please open an issue and provide as much detail as possible.
  * **Suggesting Enhancements**: Have an idea for a new feature or an improvement? Let's discuss it in an issue.
  * **Submitting Pull Requests**: If you want to fix a bug or add a feature, you can submit a pull request.

-----

## 🚀 Getting Started with Development

To get your local development environment running, you'll need Docker and `make`.

### 1\. Fork & Clone

First, fork the repository to your own GitHub account. Then, clone your fork locally:

```bash
# Replace 'your-username' with your GitHub username
git clone https://github.com/your-username/quack-cluster.git
cd quack-cluster
```

### 2\. Setup the Environment

The following commands will generate sample data and launch the cluster using Docker.

```bash
# 1. Build your container first
make build

# 2. Generate sample parquet files in the ./data directory
make data

# 3. Build and launch the cluster with 2 worker nodes
make up scale=2
```

Once the containers are running, you can monitor the cluster status via the **Ray Dashboard** at `http://localhost:8265`.

### 3\. Development Commands

The `Makefile` contains several useful commands for managing your development environment:

| Command | Description |
| :--- | :--- |
| `make up scale=N` | Starts all containers with `N` worker nodes (default: 1). |
| `make down` | Stops and removes running containers safely. |
| `make logs` | Tails the logs from all running services in real-time. |
| `make build` | Rebuilds the Docker images if you change a `Dockerfile`. |
| `make test` | Runs the `pytest` suite inside the `ray-head` container. |
| `make clean` | **DANGER:** Stops containers and deletes all associated volumes. |

-----

## ✅ Submitting a Pull Request (PR)

Ready to contribute code? Follow these steps to submit a PR.

1.  **Create a New Branch**: Create a descriptive branch name from the `main` branch.

    ```bash
    git checkout -b feature/my-awesome-feature
    ```

2.  **Make Your Changes**: Write the code for your new feature or bug fix.

3.  **Run Tests**: Before submitting, ensure all tests pass.

    ```bash
    make test
    ```

4.  **Commit Your Changes**: Use a clear and concise commit message.

    ```bash
    git commit -m "feat: Add support for my awesome feature"
    ```

5.  **Push to Your Fork**: Push your changes to your forked repository.

    ```bash
    git push origin feature/my-awesome-feature
    ```

6.  **Open a Pull Request**: Go to the original Quack-Cluster repository on GitHub and open a pull request. Provide a clear description of the changes you've made.

-----
