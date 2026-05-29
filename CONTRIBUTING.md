# Contributing to Matrix Watcher

Thank you for your interest in contributing to Matrix Watcher!

## How to Contribute

### Reporting Bugs

1. Check if the bug has already been reported in [Issues](https://github.com/amois3/matrix_watcher/issues)
2. If not, create a new issue with:
   - Clear description of the problem
   - Steps to reproduce
   - Expected vs actual behavior
   - System information (OS, Python version)

### Suggesting Features

1. Open an issue with the `enhancement` label
2. Describe the feature and its use case
3. Explain how it fits with the project philosophy

### Code Contributions

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Run tests: `pytest`
5. Commit with clear messages
6. Push and create a Pull Request

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/matrix_watcher.git
cd matrix_watcher

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Copy config
cp config.example.json config.json
```

## Code Style

- Follow PEP 8
- Use type hints where possible
- Write docstrings for public functions
- Keep functions small and focused

## Testing

- Write tests for new features
- Ensure existing tests pass
- Aim for meaningful test coverage

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src
```

## Philosophy

Remember the core principles:

1. **Honesty** - Never fake or inflate statistics
2. **Observe, don't explain** - We track correlations, not causation
3. **Simplicity** - Avoid over-engineering
4. **Transparency** - All calculations should be verifiable

## Questions?

Open an issue or reach out to the maintainers.

Thank you for contributing!
