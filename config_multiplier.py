######################################
#
# Kuklin E.
# A script for automatic generation of configuration files using tuple algorithm.
# Used for standard .ini files with fields like:
# key1 = value11 value12
# key2 = value21 value22 value23
# Let you have k parameters with n(k) values, "I" will be the list of all possible combinations:
# I = {0, 1, 2, 3, ... , n1*n2*...nk}; jk - temporal combination;
# i(k) will be the list of possible combinations for parameter k. Then:
#  ik = I mod nk
#  jk = I div nk
#  i(k-1) = jk mod n(k-1)
#  ...
#  i1 = j2 mod n1
#
######################################

import os
from loguru import logger
from math import isclose
from configparser import ConfigParser

def check_ini_sections(ini_file):
    flag = False
    try:
        with open(ini_file, "r") as file:
            for line in file:
                if line.startswith('['):
                    flag = True
    except IOError:
        logger.error("INI file corrupted")
        exit()
    if flag:
        with open(ini_file, 'r') as file:
            str_data = file.read()
        # Replace parameters if there are no signs of replacement already (for compatibility)
        if not ('"true"' in str_data or '"false"' in str_data):
            str_data = str_data.replace('"', '""')
            str_data = str_data.replace('true', '"true"')
            str_data = str_data.replace('false', '"false"')
        try:
            # Write the modified file back
            with open(ini_file, 'w') as file:
                file.write(str_data)
        except IOError:
            logger.error("INI file corrupted")
            exit()
        return True
    else:
        return False

def create_new_dict_to_work(config: ConfigParser):
    indexed_parameters = {}
    count = 0
    for section in config.sections():
        for option, value in config.items(section):
            # Remove extra spaces at the end of the line that appear due to the comment in the line
            if value[-1] == ' ':
                value = value.rstrip()
            # Remove the & symbol if a line with an important parameter was marked
            if value[-1] == '&':
                value = value[:-1]
            # Remove the extra space before the &
            if value[-1] == ' ':
                value = value.rstrip()
            indexed_parameters[count] = value
            count += 1
    return indexed_parameters

def write_file(section_flag: bool, cur_dir: str, config: ConfigParser, num_values: list,
               i: list, indexed_parameters: dict):
    if not os.path.exists(cur_dir):
        os.mkdir(cur_dir, 0o755)
    handle = open(os.path.join(cur_dir, "input.txt"), "w+")
    if not handle:
        logger.error('Cannot open new file. Exiting...')
        exit()
    count = 0
    # Then we take the name of the parameter from $parameters_sections, and the value of the parameter
    # from 2D indexed parameters[current config line][value number in the list of values of the current line]
    for section in config.sections():
        if section_flag:
            handle.write(f'[{section.title()}]\n')
        for option, value in config.items(section):
        # If the variable does not vary and is not an array (i.e. numeric values) then
        # if the variable contains true we write =true ,
        # or if the variable contains false we write =false ,
        # or then it is a constant and is written in quotes "as is"
            if num_values[count] == 1 and type(value) is not list:
                if "true" in value.lower():
                    handle.write(f'{option}=true\n')
                elif "false" in value.lower():
                    handle.write(f'{option}=false\n')
                else:
                    handle.write(f'{option}={value}\n')
            else:
                # or take the corresponding numerical parameters
                handle.write(f'{option}={indexed_parameters[count][i[count]]}\n')
            count += 1
        handle.write("\n")
    handle.close()

def main():
    ini_file = "input.txt"  # default
    if not os.path.exists(ini_file):
        logger.error('Cannot open config file')
        exit()
    else:
        logger.success('Config file was found')

    section_flag = check_ini_sections(ini_file)
    # Read file directly or add a fiction section
    if section_flag:
        config = ConfigParser()
        config.optionxform=str  # preserve registry of params
        config.read(ini_file)
    else:
        with open(ini_file, 'r') as f:
            config_string = '[dummy_section]\n' + f.read()
        config = ConfigParser()
        config.optionxform=str
        config.read_string(config_string)

    # Create a new dictionary of parameter values with numeric keys instead of parameter names
    indexed_parameters = create_new_dict_to_work(config)

    num_params = len(indexed_parameters)    # the number of parameters, this is the "k" in the formula
    num_values = []     # array of counts of values for each parameter
    combinations = 1    # count the number of combinations meanwhile

    # Go through and make some changes
    for key, value in indexed_parameters.items():
        if value[0] != '"':
            if '...' in value:
                temp = value.split("...")   # parse by three dots
                value = []
                range_value = float(temp[0])
                # Do not use direct equality comparison for float
                while isclose(range_value, float(temp[2])) or range_value < float(temp[2]):
                    value.append(f'{range_value:g}')
                    range_value += float(temp[1])
                indexed_parameters[key] = value
                temp = value
            else:
                temp = value.split(" ")
                indexed_parameters[key] = temp
        else:
            temp = value
        # len(temp) is the number of values of the current parameter
        combinations *= len(temp)       # so, this is the number of combinations in I
        num_values.append(len(temp))    # number of values for each parameter

    # Start iterating over combinations
    j = [0] * (num_params + 1)  # div - quotient with dropped remainder, J[k] = J[k+1] div nk
    i = [0] * num_params        # mod - the remainder of the division
    # "I" is a number of combination
    for I in range(combinations):
        j[num_params] = I
        for p in range(num_params - 1, 0, -1):
            i[p] = j[p + 1] % num_values[p]         # mod
            j[p] = int(j[p + 1] / num_values[p])    # div
        i[0] = j[1] % num_values[0]     # the first element, this completes the search for combination indices

        # Prepare a folder name with varying variables
        cur_dir = f'conf{str(I)}_'
        count = 0
        for section in config.sections():
            for option, value in config.items(section):
                if num_values[count] > 1:
                    cur_dir += option + "=" + indexed_parameters[count][i[count]]
                count += 1
        cur_dir = cur_dir[:100]         # cut just in case

        write_file(section_flag, cur_dir, config, num_values, i, indexed_parameters)
        logger.info(f'Processed dir: {cur_dir}')

    # Finally we finished
    logger.success('Finished')


if __name__ == '__main__':
    main()


