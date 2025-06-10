#!/usr/bin/env python3
"""
Script to generate target_configs.yml with various parameter combinations.

Usage:
    python scripts/generate_target_configs.py
    
The script generates all combinations of specified parameters and outputs
a YAML file with the configurations.
"""

import itertools
from pathlib import Path


def get_base_config():
    """Returns the base configuration template."""
    return {
        'db_parameters': {
            'frag_size': 134217728,
            'max_maps': 16,
            'max_dirty_keys': 32,
            'snapshot_written_bytes': 134217728,
            'snapshot_unload_threshold': 268435456,
            'unload_jitter_pct': 10,
            'direct_io': False  # This will be overridden
        },
        'stress_client_parameters': {
            'mixed_threads': 16,
            'write_threads': 16,
            'write_size': 512,
            'key_len': 32,
            'writes': 10000000,
            'operations': 30000000,
            'background_writes': 0,
            'no_snapshot': False,
            'report': True,
            'key_layout': 'Uniform',
            'tldr': '',
            'preserve': False,
            'read_mode': 'Get',
            'backend': 'Tidehunter',  # This will be overridden
            'path': '/home/ubuntu/working_dir',
            'read_percentage': 100  # This will be overridden
        }
    }


def format_yaml_value(value):
    """Format a Python value for YAML output."""
    if isinstance(value, bool):
        return 'true' if value else 'false'
    elif isinstance(value, str):
        # Quote strings that might need quoting
        if value == '' or ' ' in value or value.startswith('/'):
            return f'"{value}"'
        return value
    else:
        return str(value)


def config_to_yaml(config, indent=0):
    """Convert a configuration dictionary to YAML string."""
    yaml_lines = []
    prefix = '  ' * indent
    
    for key, value in config.items():
        if isinstance(value, dict):
            yaml_lines.append(f"{prefix}{key}:")
            yaml_lines.extend(config_to_yaml(value, indent + 1))
        else:
            formatted_value = format_yaml_value(value)
            yaml_lines.append(f"{prefix}{key}: {formatted_value}")
    
    return yaml_lines


def generate_configs(parameter_combinations):
    """
    Generate configurations for all parameter combinations.
    
    Args:
        parameter_combinations: Dict with parameter names as keys and lists of values as values
        
    Returns:
        List of configuration dictionaries
    """
    configs = []
    
    # Generate all combinations of parameters
    param_names = list(parameter_combinations.keys())
    param_values = list(parameter_combinations.values())
    
    for combination in itertools.product(*param_values):
        config = get_base_config()
        
        # Apply parameter values to the configuration
        for param_name, param_value in zip(param_names, combination):
            if param_name in config['db_parameters']:
                config['db_parameters'][param_name] = param_value
            elif param_name in config['stress_client_parameters']:
                config['stress_client_parameters'][param_name] = param_value
            else:
                # Handle nested parameters or add new ones
                print(f"Warning: Parameter '{param_name}' not found in base config")
        
        configs.append(config)
    
    return configs


def write_yaml_file(configs, output_path):
    """Write configurations to YAML file."""
    with open(output_path, 'w') as f:
        for i, config in enumerate(configs):
            if i > 0:
                f.write('\n')
            f.write('- ')
            yaml_lines = config_to_yaml(config)
            # First line needs special handling for list item
            if yaml_lines:
                f.write(yaml_lines[0].lstrip() + '\n')
                for line in yaml_lines[1:]:
                    f.write('  ' + line + '\n')


def main():
    """Main function to generate target configs."""
    
    # Define parameter combinations
    # You can easily modify this dictionary to add more parameters
    parameter_combinations = {
        'direct_io': [False, True],  # off and on
        'read_percentage': [100, 80, 60, 40, 20],
        'backend': ['Tidehunter', 'Rocksdb']
    }
    
    print("Generating configurations with the following parameter combinations:")
    for param, values in parameter_combinations.items():
        print(f"  {param}: {values}")
    
    total_configs = 1
    for values in parameter_combinations.values():
        total_configs *= len(values)
    print(f"\nTotal configurations to generate: {total_configs}")
    
    # Generate all configurations
    configs = generate_configs(parameter_combinations)
    
    # Write to YAML file
    output_path = Path('orchestrator/assets/target_configs.yml')
    
    # Create a backup of existing file if it exists
    if output_path.exists():
        backup_path = output_path.with_suffix('.yml.backup')
        output_path.rename(backup_path)
        print(f"Backed up existing file to: {backup_path}")
    
    write_yaml_file(configs, output_path)
    
    print(f"Generated {len(configs)} configurations in: {output_path}")
    
    # Print first few configs as preview
    print("\nPreview of first 2 configurations:")
    for i, config in enumerate(configs[:2]):
        print(f"\nConfiguration {i+1}:")
        print(f"  Backend: {config['stress_client_parameters']['backend']}")
        print(f"  Direct I/O: {config['db_parameters']['direct_io']}")
        print(f"  Read Percentage: {config['stress_client_parameters']['read_percentage']}%")


if __name__ == '__main__':
    main() 