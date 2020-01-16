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


def si_to_ip(orig_units):
    """
    Covert units when IP units system requested.

    Conversion factors sourced from ASHRAE SIGuide_Conversions.xlsx.

    Parameters:
    -----------
    orig_units : str
        Original units as reported in ESO file (should be always SI)

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
        'cop': ('COP', 'EER', 0.293),
        'ej': ('EJ', 'quad (1015 Btu)', 1.055),
        'g': ('g', 'grain (1/7000 lb)', 0.0648),
        'g/kg': ('g/kg', 'gr/lb', 0.143),
        'g/m3': ('g/m3', 'gr/gal', 17.1),
        'ha': ('ha', 'acre (43,560 ft2)', 0.4047),
        'j': ('J', 'Btu (International Table)', 1055.056),
        'j/kg': ('J/kg', 'ft-lbf/lb (specific energy)', 2.99),
        'j/m2': ('J/m2', 'Btu/ft2 (International Table)', 11356.53),
        'j/m3': ('J/m3', 'Btu/ft3 (International Table)', 37258.951),
        'kg': ('kg', 'lb (avoirdupois, mass)', 0.453592),
        'kg/(pa-s-m)': ('kg/(Pa-s-m)', 'perm inch (permeability at 32°F)', 1.45362e-12),
        'kg/(pa-s-m2)': ('kg/(Pa-s-m2)', 'perm (permeance at 32°F)', 5.72135e-11),
        'kg/m2': ('kg/m2', 'lb/ft2', 4.88),
        'kg/m3': ('kg/m3', 'lb/ft3 (density r)', 16.0),
        'kg/s': ('kg/s', 'lb/min', 0.007559),
        'kj/(kg-k)': ('kJ/(kg-K)', 'Btu/lb-°F (specific heat cp)', 4.1868),
        'kj/kg': ('kJ/kg', 'Btu/lb', 2.326),
        'kj/m3': ('kJ/m3', 'kW/1000 cfm', 2.11888),
        'km': ('km', 'mile', 1.609),
        'km/h': ('km/h', 'mile per hour (mph)', 1.609344),
        'kn': ('kN', 'kip (1000 lbf)', 4.45),
        'kpa': ('kPa', 'psi', 6.895),
        'l': ('L', 'ft3', 28.316846),
        'l/(s-m2)': ('L/(s-m2)', 'gpm/ft2', 0.6791),
        'l/s': ('L/s', 'ft3/min, cfm', 0.471947),
        'lx': ('lx', 'footcandle', 10.76391),
        'm': ('m', 'ft', 0.3048),
        'm/s': ('m/s', 'ft/min, fpm', 0.00508),
        'm2': ('m2', 'ft2', 0.092903),
        'm2 ': ('m2', 'acre (43,560 ft2)', 4046.873),
        '(m2-k)/w': ('(m2-K)/W', 'ft2-h-°F/Btu (thermal resistance R)', 0.17611),
        'm3': ('m3', 'ft3', 0.02832),
        'mg': ('Mg', 'ton, long (2240 lb)', 1.016046),
        'mg/kg': ('mg/kg', 'ppm (by mass)', 1.0),
        'mg; t (tonne)': ('Mg; t (tonne)', 'ton, short (2000 lb)', 0.907184),
        'ml': ('mL', 'ounce (liquid, U.S.)', 29.6),
        'ml/j': ('mL/J', 'gpm/ton refrigeration', 0.0179),
        'ml/s': ('mL/s', 'gph', 1.05),
        'mm': ('mm', 'inch', 25.4),
        'mm/m': ('mm/m', 'in/100 ft, thermal expansion coefficient', 0.833),
        'mm2': ('mm2', 'in2', 645.16),
        'mm2/s': ('mm2/s', 'ft2/s (kinematic viscosity n)', 92900.0),
        'mm3': ('mm3', 'in3 (section modulus)', 16387.0),
        'mm4': ('mm4', 'in4 (section moment)', 416231.0),
        'mn-m': ('mN-m', 'in-lbf (torque or moment)', 113.0),
        'mpa': ('MPa', 'kip/in2 (ksi)', 6.895),
        'mpa-s': ('mPa-s', 'lb/ft-h (dynamic viscosity m)', 0.4134),
        'n': ('N', 'kilopond (kg force)', 9.81),
        'n/m': ('N/m', 'lbf/ft (uniform load)', 14.5939),
        'n-m': ('N-m', 'ft-lbf (torque or moment)', 1.355818),
        'pa': ('Pa', 'in. of water (60°F)', 248.84),
        'pa/m': ('Pa/m', 'ft of water per 100 ft pipe', 98.1),
        'pa-s': ('Pa-s', 'lbf-s/ft2 (dynamic viscosity m)', 47.88026),
        'w/(m-k)': ('W/(m-K)', 'Btu-ft/h-ft2-°F', 1.730735),
        'w/(m2-k)': ('W/(m2-K)', 'Btu/h-ft2-°F (overall heat transfer coefficient U)', 5.678263),
        "c": ("C", "F", c_to_fahrenheit),
    }

    request = orig_units.lower()

    try:
        return table[request]

    except KeyError:
        print("Cannot convert to IP, original units [{}] left!".format(orig_units))

# duplicates = {
# 'g': ('g', 'ounce (mass, avoirdupois)', 28.35),
# 'g': ('g', 'lb (avoirdupois, mass)', 453.592),
# 'j': ('J', 'Btu (thermochemical)', 1054.35),
# 'j': ('J', 'calorie (thermochemical)', 4.184),
# 'j': ('J', 'ft·lbf (work)', 1.356),
# 'j/m3': ('J/m3', 'Btu/gal', 278717.1765),
# 'kg/m3': ('kg/m3', 'ounce (avoirdupois) per gallon', 7.489152),
# 'kg/m3': ('kg/m3', 'lb/gallon', 120.0),
# 'kg/s': ('kg/s', 'lb/h', 0.000126),
# 'km': ('km', 'mile, nautical', 1.852),
# 'kpa': ('kPa', 'atmosphere (standard)', 101.325),
# 'kpa': ('kPa', 'bar', 100.0),
# 'kpa': ('kPa', 'in. of mercury (60°F)', 3.3864),
# 'kpa': ('kPa', 'millibar', 0.1),
# 'kpa': ('kPa', 'mm of mercury (60°F)', 0.133),
# 'kw': ('kW', 'horsepower (boiler) (33, 470 Btu/h)', 9.81),
# 'kw': ('kW', 'horsepower (550 ft·lbf/s)', 0.7457),
# 'kw': ('kW', 'lb/h [steam at 212°F (100°C)]', 0.2843),
# 'kw': ('kW', 'ton, refrigeration (12,000 Btu/h)', 3.517),
# 'l': ('L', 'barrel (42 U.S. gal, petroleum)', 159.0),
# 'l': ('L', 'gallon (U.S., 231 in3)', 3.785412),
# 'l': ('L', 'quart (liquid, U.S.)', 0.9463),
# 'l/s': ('L/s', 'ft3/s, cfs', 28.316845),
# 'l/s': ('L/s', 'gpm', 0.0631),
# 'm': ('m', 'yd', 0.9144),
# 'm/s': ('m/s', 'ft/s, fps', 0.3048),
# 'm/s': ('m/s', 'mile per hour (mph)', 0.447),
# 'm2': ('m2', 'square (100 ft2)', 9.2903),
# 'm2': ('m2', 'yd2', 0.8361),
# '(m2·k)/w': ('(m2·K)/W', 'clo', 0.155),
# 'm3': ('m3', 'barrel (42 U.S. gal, petroleum)', 0.1580987),
# 'm3': ('m3', 'bushel (dry, U.S.)', 0.0352394),
# 'm3': ('m3', 'litre', 0.001),
# 'm3': ('m3', 'pint (liquid, U.S.)', 0.000473176),
# 'm3': ('m3', 'yd3', 0.7646),
# 'mj': ('MJ', 'kWh', 3.6),
# 'mj': ('MJ', 'therm (U.S.)', 105.5),
# 'ml': ('mL', 'in3 (volume)', 16.3874),
# 'ml': ('mL', 'tablespoon (approximately)', 15.0),
# 'ml': ('mL', 'teaspoon (approximately)', 5.0),
# 'ml/s': ('mL/s', 'in3/min (SCIM)', 0.273117),
# 'mm': ('mm', 'ft', 304.8),
# 'mm2/s': ('mm2/s', 'centistokes (kinematic viscosity n)', 1.0),
# 'mn·m': ('mN·m', 'ounce inch (torque, moment)', 7.06),
# 'mpa': ('mPa', 'micron (mm) of mercury (60°F)', 133.0),
# 'mpa·s': ('mPa·s', 'centipoise (dynamic viscosity m)', 1.0),
# 'mpa·s': ('mPa·s', 'lb/ft·s (dynamic viscosity m)', 1490.0),
# 'n': ('N', 'dyne', 1e-05),
# 'n': ('N', 'ounce (force or thrust)', 0.278),
# 'n': ('N', 'lbf (force or thrust)', 4.448222),
# 'pa': ('Pa', 'dyne/cm2', 0.1),
# 'pa': ('Pa', 'ft of water', 2989.0),
# 'pa': ('Pa', 'mm of water (60°F)', 9.8),
# 'pa': ('Pa', 'lbf/ft2', 47.9),
# 'pa': ('Pa', 'torr (1 mm Hg at 0°C)', 133.0),
# 'w': ('W', 'Btu/h', 0.2930711),
# 'w': ('W', 'EDR hot water (150 Btu/h)', 43.9606),
# 'w': ('W', 'EDR steam (240 Btu/h)', 70.33706),
# 'w': ('W', 'ft·lbf/min (power)', 0.0226),
# 'w/(m·k)': ('W/(m·K)', 'Btu·in/h·ft2·°F (thermal conductivity k) .', 0.1442279),
# 'w/m2': ('W/m2', 'met', 58.15),
# 'w/m2': ('W/m2', 'watt per square foot', 10.76),
# 'w/m2': ('W/m2', 'Btu/h-ft2', 3.154591),

# }
