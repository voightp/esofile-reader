from outputs import TimestepOutputs, HourlyOutputs, DailyOutputs


def convert(data, units_system, rate_units, energy_units):
    """ Convert raw E+ results to use requested units. """

    orig_units = data.columns.get_level_values("units")
    conv_factor = 1
    new_units = orig_units

    # Orig_units might be a list if the
    # data represents global or time step results
    dummy_units = orig_units if len(orig_units) == 1 else orig_units[0]

    # Convert 'Rate' results
    if dummy_units == "W" and rate_units != "W":
        new_units = rate_units
        conv_factor = convert_rate(rate_units)

    # Convert 'Rate/m2' results
    if dummy_units == "W/m2" and rate_units != "W":
        new_units = rate_units + "/m2"
        conv_factor = convert_rate(rate_units)

    # Convert 'Energy' results
    if dummy_units == "J" and energy_units != "J":
        new_units = energy_units
        conv_factor = convert_energy(energy_units)

    # Convert 'Energy/m2' results
    if dummy_units == "J/m2" and energy_units != "J":
        new_units = energy_units + "/m2"
        conv_factor = convert_energy(energy_units)

    # Convert 'SI' default to 'IP'
    # 'Energy' and 'Rate' units are independent on  units system
    if units_system == "IP":
        new_units, conv_factor = convert_to_ip(dummy_units)
        if dummy_units == "C":
            data.columns.set_levels(["F"], level="units", inplace=True)
            data = data.apply(c_to_fahrenheit)

    if conv_factor != 1:
        # Convert values
        data = _convert(data, conv_factor, dummy_units)
        # Replace original units
        data.columns.set_levels([new_units], level="units", inplace=True)
        return data

    else:
        # Return original data
        return data


def _cnd(data, level, val):
    """ Return Boolean array to filter DataFrame values. """
    arr = data.columns.get_level_values(level) == val
    return arr


def _convert(data, conv_factor, orig_units):
    """ Convert values for columns using specified units. """
    if data.columns.nlevels == 5:
        # Only values must be converted for peak results
        data.loc[:,
        (_cnd(data, "units", orig_units)) & _cnd(data, "data", "value")
        ] = data.loc[:, (_cnd(data, "units", orig_units)) & _cnd(data, "data", "value")] / conv_factor

    else:
        data.loc[:, (_cnd(data, "units", orig_units))] = data.loc[:, (_cnd(data, "units", orig_units))] / conv_factor

    return data


def _timestep_multiplier(data_set):
    """ Get a number of timesteps in hour. """
    timestamps = data_set.index
    timedelta = timestamps[1] - timestamps[0]
    n_steps = 3600 / timedelta.seconds
    return n_steps


def energy_to_rate(data, data_set, start_date, end_date):
    units = data.columns.get_level_values("units")
    if units == "J":
        new_units = "W"
        if isinstance(data_set, TimestepOutputs):
            n_steps = _timestep_multiplier(data_set)
            data = data * n_steps

        elif isinstance(data_set, HourlyOutputs):
            pass

        elif isinstance(data_set, DailyOutputs):
            denominator = 24
            data = data / denominator  # Convert J to J/h

        else:
            denominator = data_set["Number of days"] * 24
            sliced_denominator = denominator[start_date:end_date]
            data = data.div(sliced_denominator.values, axis=0)  # Convert J to J/h

        data = data / 3600  # Convert J/h -> W
        data.columns.set_levels([new_units], level="units", inplace=True)

    return data


def rate_to_energy(data, data_set, start_date, end_date):
    units = data.columns.get_level_values("units")
    if units == "W" or units == "W/m2":
        new_units = "J/m2" if units == "W/m2" else "J"

        if isinstance(data_set, TimestepOutputs):
            n_steps = _timestep_multiplier(data_set)
            data = data / n_steps

        elif isinstance(data_set, HourlyOutputs):
            pass

        if isinstance(data_set, DailyOutputs):
            multiplier = 24
            data = data * multiplier  # Convert W to Wh

        else:
            multiplier = data_set["Number of days"] * 24
            sliced_multiplier = multiplier[start_date:end_date]
            data = data.mul(sliced_multiplier.values, axis=0)  # Convert W -> Wh

        data = data * 3600  # Convert Wh -> J
        data.columns.set_levels([new_units], level="units", inplace=True)

    return data


def convert_rate(rate_units):
    """
    Find conversion rate for given rate units.

    EnergyPlus standard units for power are 'Watts'.

    Conversion table:
        W       ->      Btu/h       /       0.2930711
        W       ->      kBtu/h      /       293.0711
        W       ->      MBtu/h      /       293 071.1
        W       ->      kW          /       1000
        W       ->      MW          /       1000 000
    """
    request = rate_units.lower()

    table = {
        "btu/h": 0.2930711,
        "kbtu/h": 293.0711,
        "mbtu": 293071.1,
        "kw": 1000,
        "mw": 1000000,
    }

    try:
        conversion_factor = table[request]

    except KeyError:
        print("Specified rate units [{}] not applicable!".format(rate_units))
        conversion_factor = 1

    return conversion_factor


def convert_energy(energy_units):
    """
    Find conversion rate for given energy units.

    EnergyPlus standard units for power are 'Joules'.

    Conversion table:
        J       ->      Wh          /       3600
        J       ->      kWh         /       3 600 000
        J       ->      MWh         /       3 600 000 000
        J       ->      kJ          /       1 000
        J       ->      MJ          /       1000 000
        J       ->      GJ          /       1000 000 000
        J       ->      Btu         /       1055.056
        J       ->      kBtu        /       1 055 056
    """
    request = energy_units.lower()

    table = {
        "wh": 3600,
        "kwh": 3600000,
        "mwh": 3600000000,
        "kj": 1000,
        "mj": 1000000,
        "gj": 1000000000,
        "btu": 1055.056,
        "kbtu": 1055056,
        "mbtu": 1055056000,
    }

    try:
        conversion_factor = table[request]

    except KeyError:
        print("Specified energy units [{}] not applicable!".format(energy_units))
        conversion_factor = 1

    return conversion_factor


def c_to_fahrenheit(val):
    return val * 1.8 + 32


def convert_to_ip(orig_units):
    """
    Covert units when IP units system requested.

    Conversion table:

        m           ->
        m2          ->
        m3          ->
        deltaC      ->      deltaF
        m/s         ->      ft/min
        kg          ->      lb
        kg/s        ->      lb/min      /       0.007559
        m3/s        ->      g/min
        Pa          ->
        J/kg        ->
        kg/m3       ->      lb/f3
        W/m2        ->
        J/kg.K      ->
        W/m.K       ->
        m2/s        ->
        W/m2-K      ->
        m2-K/W      ->
        lx          ->
        lm          ->
        cd          ->
        cd/m2       ->

    Parameters:
    -----------
    orig_units : str
        Original units as reported in ESO file (should be always SI)
    units_system : {'SI','IP'}
        Requested units system.

    Returns:
    --------
    str
        Converted units.
    float
        A conversion factor between original and
        returned units.

    """
    request = orig_units.lower()
    table = {
        "w": ("Btu/h", 0.2930711),
        "w/m2": ("Btu/h.ft2", 5.678263),
    }

    try:
        units_factor = table[request]

    except KeyError:
        print("Cannot convert to IP, original units [{}] left!".format(orig_units))
        units_factor = (orig_units, 1)

    return units_factor
