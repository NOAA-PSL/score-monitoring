"""Copyright 2024 NOAA
All rights reserved.

Methods to calculate gridcell area weighted statistics
The functions generally take the following two input variables:
    xarray_variable - xarray variable data
    gridcell_area_data - xarray variable containing gridcell areas, in the
    same shape as xarray_variable
"""

import numpy as np

def area_weighted_mean(xarray_variable, gridcell_area_weights):  
    """returns the gridcell area weighted mean of xarray_variable and checks
    that gridcell_area_weights are valid
    """
    weighted_mean, sumweights = np.ma.average(xarray_variable,
                                              weights=gridcell_area_weights,
                                              returned=True)

    try:
        assert sumweights >= 0.999 * 4. * np.pi
        assert sumweights <= 1.001 * 4. * np.pi
    except AssertionError as err:
        msg = (f'{gridcell_area_weights}\n'
               '(gridcell area weights) sum does not equal 4pi steradians; ' 
               'cannot calculate accurate global/regional weighted statistics')
        raise AssertionError(msg) from err
    return(weighted_mean)

def area_weighted_variance(xarray_variable, gridcell_area_weights,
                           expected_value=None):
    """returns the gridcell weighted variance of the requested variables using
    the following formula:
                        
        variance = sum_R{ w_i * (x_i - xbar)^2 },
    
    where sum_R represents the summation for each value x_i over the region of 
    interest R with normalized gridcell area weights w_i and weighted mean xbar
    """
    if expected_value == None:
        expected_value = calculate_weighted_means(xarray_variable,
                                                  gridcell_area_weights)
    
    weighted_variance = -expected_value**2 + np.ma.sum(
                                               xarray_variable**2 * 
                                               (gridcell_area_weights / 
                                                gridcell_area_weights.sum()))
    return(weighted_variance)