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
        W/m2        ->      W/ft2       /       0.092903
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

    table = {
        "kg/s": ("kg/s", "lb/min", 0.007559),  # TODO add IP data, finish docstrings
        "c": ("C", "F", c_to_fahrenheit),
    }

    request = orig_units.lower()

    try:
        return table[request]

    except KeyError:
        print("Cannot convert to IP, original units [{}] left!".format(orig_units))


def energy_table(new_units, per_area=False):
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
        J       ->      MBtu        /       1 055 056 000
    """

    table = {
        "wh": ("J", "Wh", 3600),
        "kwh": ("J", "kWh", 3600000),
        "mwh": ("J", "MWh", 3600000000),
        "kj": ("J", "kJ", 1000),
        "mj": ("J", "MJ", 1000000),
        "gj": ("J", "GJ", 1000000000),
        "btu": ("J", "Btu", 1055.056),
        "kbtu": ("J", "kBtu", 1055056),
        "mbtu": ("J", "MBtu", 1055056000),
    }

    table_pa = {
        "wh": ("J/m2", "Wh/m2", 3600),
        "kwh": ("J/m2", "kWh/m2", 3600000),
        "mwh": ("MWh/m2", 3600000000),
        "kj": ("J/m2", "kJ/m2", 1000),
        "mj": ("J/m2", "MJ/m2", 1000000),
        "gj": ("J/m2", "GJ/m2", 1000000000),
        "btu": ("J/m2", "Btu/f2", 1055.056 / 10.76391),  # TODO verify this!
        "kbtu": ("J/m2", "kBtu/f2", 1055056 / 10.76391),
        "mbtu": ("J/m2", "MBtu/f2", 1055056000 / 10.76391),
    }

    request = new_units.lower()

    try:
        tbl = table_pa if per_area else table
        return tbl[request]
    except KeyError:
        print("Specified energy units [{}] not applicable!".format(new_units))


def rate_table(new_units, per_area=False):
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

    table = {
        "kw": ("W", "kW", 1000),
        "mw": ("W", "MW", 1000000),
        "btu/h": ("W", "Btu/h", 0.2930711),
        "kbtu/h": ("W", "kBtu/h", 293.0711),
        "mbtu/h": ("W", "MBtu/h", 293071.1),
    }

    table_pa = {
        "kw": ("W/m2", "kW/m2", 1000),
        "mw": ("W/m2", "MW/m2", 1000000),
        "btu/h": ("W/m2", "Btu/h-ft2", 0.2930711 / 10.76391),  # TODO verify this!
        "kbtu/h": ("W/m2", "kBtu/h-ft2", 293.0711 / 10.76391),
        "mbtu/h": ("W/m2", "MBtu/h-ft2", 293071.1 / 10.76391),
    }

    request = new_units.lower()

    try:
        tbl = table_pa if per_area else table
        return tbl[request]
    except KeyError:
        print("Specified rate units [{}] not applicable!".format(new_units))


categories = {
    "Air System",
    "Debug Surface Solar Shading Model",
    "Electric Load Center",
    "Environmental Impact",
    "Facility",
    "Generator",
    "HVAC System",
    "Inverter",
    "Lights",
    "Other Equipment",
    "People",
    "Schedule",
    "Site",
    "Surface",
    "System Node",
    "Water Use Equipment",
    "Zone",
}

summed_units = [
    "J",
    "J/m2"
]

averaged_units = [
    "W",
    "W/m2",
    "C"
]
