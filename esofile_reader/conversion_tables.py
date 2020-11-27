J_TO_WH = 0.000277777777777778
J_TO_BTU = 0.000947817
W_TO_BTUH = 3.4121412858518

TO_KILO = 1e-3
TO_MEGA = 1e-6
TO_GIGA = 1e-9

M2_TO_FT2 = 10.7639104167097

# Conversion table:
#     J       =>      Wh          *       0.000277777777777778
#     J       =>      kWh         *       0.000277777777777778 * 1e-3
#     J       =>      MWh         *       0.000277777777777778 * 1e-6
#     J       =>      kJ          *       1e-3
#     J       =>      MJ          *       1e-6
#     J       =>      GJ          *       1e-9
#     J       =>      Btu         *       0.000947817
#     J       =>      kBtu        *       0.000947817 * 1e-3
#     J       =>      MBtu        *       0.000947817 * 1e-6


ENERGY_TABLE = {
    "Wh": ("Wh", J_TO_WH),
    "kWh": ("kWh", J_TO_WH * TO_KILO),
    "MWh": ("MWh", J_TO_WH * TO_MEGA),
    "kJ": ("kJ", TO_KILO),
    "MJ": ("MJ", TO_MEGA),
    "GJ": ("GJ", TO_GIGA),
    "Btu": ("Btu", J_TO_BTU),
    "kBtu": ("kBtu", J_TO_BTU * TO_KILO),
    "MBtu": ("MBtu", J_TO_BTU * TO_MEGA),
}

ENERGY_TABLE_PER_AREA = {
    "Wh": ("Wh/m2", J_TO_WH),
    "kWh": ("kWh/m2", J_TO_WH * TO_KILO),
    "MWh": ("MWh/m2", J_TO_WH * TO_MEGA),
    "kJ": ("kJ/m2", TO_KILO),
    "MJ": ("MJ/m2", TO_MEGA),
    "GJ": ("GJ/m2", TO_GIGA),
    "Btu": ("Btu/ft2", J_TO_BTU / M2_TO_FT2),
    "kBtu": ("kBtu/ft2", J_TO_BTU / M2_TO_FT2 * TO_KILO),
    "MBtu": ("MBtu/ft2", J_TO_BTU / M2_TO_FT2 * TO_MEGA),
}

# Conversion table:
#     W       =>      kW          *       10e-3
#     W       =>      MW          *       10e-6
#     W       =>      Btu/h       *       3.4121412858518
#     W       =>      kBtu/h      *       3.4121412858518 * 10e-3
#     W       =>      MBtu/h      *       3.4121412858518 * 10e-6


RATE_TABLE = {
    "kW": ("kW", TO_KILO),
    "MW": ("MW", TO_MEGA),
    "Btu/h": ("Btu/h", W_TO_BTUH),
    "kBtu/h": ("kBtu/h", W_TO_BTUH * TO_KILO),
    "MBtu/h": ("MBtu/h", W_TO_BTUH * TO_MEGA),
}

RATE_TABLE_PER_AREA = {
    "kW": ("kW/m2", TO_KILO),
    "MW": ("MW/m2", TO_MEGA),
    "Btu/h": ("Btu/h-ft2", W_TO_BTUH / M2_TO_FT2),
    "kBtu/h": ("kBtu/h-ft2", W_TO_BTUH / M2_TO_FT2 * TO_KILO),
    "MBtu/h": ("MBtu/h-ft2", W_TO_BTUH / M2_TO_FT2 * TO_MEGA),
}

_SI_IP = {
    "$/(m3/s)": ("$/(ft3/min)", 0.000472000059660808),
    "$/(W/K)": ("$/(Btu/h-F)", 0.52667614683731),
    "$/kW": ("$/(kBtuh/h)", 0.293083235638921),
    "$/m2": ("$/ft2", 0.0928939733269818),
    "$/m3": ("$/ft3", 0.0283127014102352),
    "(kg/s)/W": ("(lbm/sec)/(Btu/hr)", 0.646078115385742),
    "1/K": ("1/F", 0.555555555555556),
    "1/m": ("1/ft", 0.3048),
    "A/K": ("A/F", 0.555555555555556),
    "cm": ("in", 0.3937),
    "cm2": ("inch2", 0.15500031000062),
    "deltaC": ("deltaF", 1.8),
    "deltaC/hr": ("deltaF/hr", 1.8),
    "deltaJ/kg": ("deltaBtu/lb", 0.0004299),
    "g/GJ": ("lb/MWh", 0.00793664091373665),
    "g/kg": ("grains/lb", 7),
    "g/MJ": ("lb/MWh", 7.93664091373665),
    "g/mol": ("lb/mol", 0.0022046),
    "g/m-s": ("lb/ft-s", 0.000671968949659),
    "g/m-s-K": ("lb/ft-s-F", 0.000373574867724868),
    "GJ": ("ton-hrs", 78.9889415481832),
    "J/K": ("Btu/F", 526.565),
    "J/kg-K": ("Btu/lb-F", 0.000239005736137667),
    "J/kg-K2": ("Btu/lb-F2", 0.000132889924714692),
    "J/kg-K3": ("Btu/lb-F3", 7.38277359526066e-05),
    "J/m2-K": ("Btu/ft2-F", 4.89224766847393e-05),
    "J/m3": ("Btu/ft3", 2.68096514745308e-05),
    "J/m3-K": ("Btu/ft3-F", 1.49237004739337e-05),
    "K": ("R", 1.8),
    "K/m": ("F/ft", 0.54861322767449),
    "kg": ("lb", 2.2046),
    "kg/J": ("lb/Btu", 2325.83774250441),
    "kg/kg-K": ("lb/lb-F", 0.555555555555556),
    "kg/m": ("lb/ft", 0.67196893069637),
    "kg/m2": ("lb/ft2", 0.204794053596664),
    "kg/m3": ("lb/ft3", 0.062428),
    "kg/m-s": ("lb/ft-s", 0.67196893069637),
    "kg/m-s-K": ("lb/ft-s-F", 0.373316072609094),
    "kg/m-s-K2": ("lb/ft-s-F2", 0.207397818116164),
    "kg/Pa-s-m2": ("lb/psi-s-ft2", 1412.00523459398),
    "kg/s": ("lb/s", 2.20462247603796),
    "kg/s2": ("lb/s2", 2.2046),
    "kg/s-m": ("lb/s-ft", 0.67196893069637),
    "kJ/kg": ("Btu/lb", 0.429925),
    "kPa": ("psi", 0.145038),
    "L/day": ("pint/day", 2.11337629827348),
    "L/GJ": ("gal/kWh", 0.000951022349025202),
    "L/kWh": ("pint/kWh", 2.11337629827348),
    "L/MJ": ("gal/kWh", 0.951022349025202),
    "lux": ("foot-candles", 0.092902267),
    "m": ("ft", 3.28083989501312),
    "m/hr": ("ft/hr", 3.28083989501312),
    "m/s": ("ft/min", 196.850393700787),
    "m/yr": ("inch/yr", 39.3700787401575),
    "m2": ("ft2", 10.7639104167097),
    "m2/m": ("ft2/ft", 3.28083989501312),
    "m2/person": ("ft2/person", 10.764961),
    "m2/s": ("ft2/s", 10.7639104167097),
    "m2-K/W": ("ft2-F-hr/Btu", 5.678263),
    "m3": ("gal", 264.172037284185),
    "m3/GJ": ("ft3/MWh", 127.13292),
    "m3/hr": ("ft3/hr", 35.3146667214886),
    "m3/hr-m2": ("ft3/hr-ft2", 3.28083989501312),
    "m3/hr-person": ("ft3/hr-person", 35.3146667214886),
    "m3/kg": ("ft3/lb", 16.018),
    "m3/m2": ("ft3/ft2", 3.28083989501312),
    "m3/MJ": ("ft3/kWh", 127.13292),
    "m3/person": ("ft3/person", 35.3146667214886),
    "m3/s": ("ft3/min", 2118.88000328931),
    "m3/s-m": ("ft3/min-ft", 645.89),
    "m3/s-m2": ("ft3/min-ft2", 196.85),
    "m3/s-person": ("ft3/min-person", 2118.6438),
    "m3/s-W": ("(ft3/min)/(Btu/h)", 621.099127332943),
    "N-m": ("lbf-in", 8.85074900525547),
    "N-s/m2": ("lbf-s/ft2", 0.0208857913669065),
    "Pa": ("psi", 0.000145037743897283),
    "percent/K": ("percent/F", 0.555555555555556),
    "person/m2": ("person/ft2", 0.0928939733269818),
    "s/m": ("s/ft", 0.3048),
    "V/K": ("V/F", 0.555555555555556),
    "W/((m3/s)-Pa)": ("W/((gal/min)-ftH20)", 0.188582274697355),
    "W/(m3/s)": ("W/(ft3/min)", 0.0004719475),
    "W/K": ("Btu/h-F", 1.89563404769544),
    "W/m": ("Btu/h-ft", 1.04072),
    "W/m2": ("W/sqf", 1 / M2_TO_FT2),
    "W/m2-K": ("Btu/h-ft2-F", 0.176110194261872),
    "W/m2-K2": ("Btu/h-ft2-F2", 0.097826),
    "W/m-K": ("Btu-in/h-ft2-F", 6.93481276005548),
    "W/m-K2": ("Btu/h-F2-ft", 0.321418310071648),
    "W/m-K3": ("Btu/h-F3-ft", 0.178565727817582),
    "W/person": ("Btu/h-person", 3.4121412858518),
}


def c_to_fahrenheit(val):
    return val * 1.8 + 32


def j_kg_to_btu_lb(val):
    return val * 0.00042986 + 7.686


SI_TO_IP = {"C": ("F", c_to_fahrenheit), "J/kg": ("Btu/lb", j_kg_to_btu_lb), **_SI_IP}
