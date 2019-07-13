def si_to_ip(orig_units):
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

    def c_to_fahrenheit(val):
        return val * 1.8 + 32

    request = orig_units.lower()
    table = {
        "kg/s": ("lb/min", 0.007559),  # TODO add IP data, finish docstrings
        "c": ("F", c_to_fahrenheit),
    }

    try:
        conv_tuple = table[request]

    except KeyError:
        print("Cannot convert to IP, original units [{}] left!".format(orig_units))
        conv_tuple = (orig_units, None, 1)

    return conv_tuple


def energy_table(units_system, new_units, per_area=False):
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
        J       ->      kBtu        /       1 055 056 000
    """
    request = new_units.lower()

    table = {
        "SI": {
            "wh": 3600,
            "kwh": 3600000,
            "mwh": 3600000000,
            "kj": 1000,
            "mj": 1000000,
            "gj": 1000000000},
        "IP": {
            "btu": 1055.056,
            "kbtu": 1055056,
            "mbtu": 1055056000,
        }}

    table_pa = {
        "SI": {
            "wh": ("Wh/m2", 3600),
            "kwh": ("kWh/m2", 3600000),
            "mwh": ("MWh/m2", 3600000000),
            "kj": ("kJ/m2", 1000),
            "mj": ("MJ/m2", 1000000),
            "gj": ("GJ/m2", 1000000000)},
        "IP": {
            "btu": ("btu/f2", 1055.056 / 10.76391),  # TODO verify this!
            "kbtu": ("kBtu/f2", 1055056 / 10.76391),
            "mbtu": ("mBtu/f2", 1055056000 / 10.76391),
        }
    }

    try:
        tbl = table_pa if per_area else table
        return tbl[units_system][request]
    except KeyError:
        print("Specified energy units [{}] not applicable!".format(new_units))


def rate_table(units_system, new_units, per_area=False):
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
    request = new_units.lower()

    table = {
        "SI": {
            "kw": 1000,
            "mw": 1000000},
        "IP": {
            "btu/h": 0.2930711,
            "kbtu/h": 293.0711,
            "mbtu/h": 293071.1,
        }
    }

    table_pa = {
        "SI": {
            "kw/m2": 1000,
            "mw/m2": 1000000
        },
        "IP": {
            "w/ft2": 1 / 10.76391,
            "btu/h-ft2": 0.2930711 / 10.76391,  # TODO verify this!
            "kbtu/h-ft2": 293.0711 / 10.76391,
            "mbtu/h-ft2": 293071.1 / 10.76391,
        }
    }

    try:
        tbl = table_pa if per_area else table
        return tbl[units_system][request]
    except KeyError:
        print("Specified energy units [{}] not applicable!".format(new_units))
