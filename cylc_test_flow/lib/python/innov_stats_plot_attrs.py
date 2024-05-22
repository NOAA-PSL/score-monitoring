from collections import namedtuple

PlotAttrs = namedtuple(
    'PlotAttrs',
    [
        'metric',
        'stat',
        'axes_attrs',
        'legend',
        'xlabel',
        'ylabel'
    ],
)

AxesAttrs = namedtuple(
    'AxesAttrs',
    [
        'xmin',
        'xmax',
        'xint',
        'ymin',
        'ymax',
        'yint'
    ]
)

LegendData = namedtuple(
    'LegendData',
    [
        'loc',
        'fancybox',
        'edgecolor',
        'framealpha',
        'shadow',
        'fontsize',
        'facecolor'
    ]
)

AxesLabel = namedtuple(
    'AxesLabel',
    [
        'axis',
        'label',
        'horizontalalignment'
    ]
)

DEFAULT_LEGEND_ATTRS = LegendData(
    loc='upper right',
    fancybox=None,
    edgecolor=None,
    framealpha=None,
    shadow=None,
    fontsize='x-small',
    facecolor=None
)

DEFAULT_YLABEL_ATMOS = AxesLabel(
    axis='y',
    label='Pressure (hPa)',
    horizontalalignment='center'
)


plot_attrs = {
    'spechumid_bias': PlotAttrs(
        metric='spechumid',
        stat='bias',
        axes_attrs=AxesAttrs(
            xmin=-1.e-3,
            xmax=1.e-3,
            xint=0.0005,
            ymin=200.0,
            ymax=900.0,
            yint=100.0
        ),
        legend=DEFAULT_LEGEND_ATTRS,
        xlabel=AxesLabel(
            axis='x',
            label='First-Guess Specific Humidity Bias (g kg$^{-1}$)',
            horizontalalignment='center'
        ),
        ylabel=DEFAULT_YLABEL_ATMOS
    ),
    'spechumid_rmsd': PlotAttrs(
        metric='spechumid',
        stat='rmsd',
        axes_attrs=AxesAttrs(
            xmin=-1.e-3,
            xmax=1.e-3,
            xint=0.0005,
            ymin=200.0,
            ymax=900.0,
            yint=100.0
        ),
        legend=DEFAULT_LEGEND_ATTRS,
        xlabel=AxesLabel(
            axis='x',
            label='First-Guess Specific Humidity Bias (g kg$^{-1}$)',
            horizontalalignment='center'
        ),
        ylabel=DEFAULT_YLABEL_ATMOS
    ),
    'temperature_bias': PlotAttrs(
        metric='temperature',
        stat='bias',
        axes_attrs=AxesAttrs(
            xmin=-2.0,
            xmax=2.0,
            xint=0.5,
            ymin=200.0,
            ymax=900.0,
            yint=100.0
        ),
        legend=DEFAULT_LEGEND_ATTRS,
        xlabel=AxesLabel(
            axis='x',
            label='First-Guess Temperature Bias (K)',
            horizontalalignment='center'
        ),
        ylabel=DEFAULT_YLABEL_ATMOS
    ),
    'temperature_rmsd': PlotAttrs(
        metric='temperature',
        stat='rmsd',
        axes_attrs=AxesAttrs(
            xmin=0.0,
            xmax=2.0,
            xint=0.25,
            ymin=200.0,
            ymax=900.0,
            yint=100.0
        ),
        legend=DEFAULT_LEGEND_ATTRS,
        xlabel=AxesLabel(
            axis='x',
            label='First-Guess Temperature Bias (K)',
            horizontalalignment='center'
        ),
        ylabel=DEFAULT_YLABEL_ATMOS
    ),
    'uvwind_bias': PlotAttrs(
        metric='uvwind',
        stat='bias',
        axes_attrs=AxesAttrs(
            xmin=-5.0,
            xmax=5.0,
            xint=1.0,
            ymin=200.0,
            ymax=900.0,
            yint=100.0
        ),
        legend=DEFAULT_LEGEND_ATTRS,
        xlabel=AxesLabel(
            axis='x',
            label='First-Guess Wind Bias (m s$^{-1}$)',
            horizontalalignment='center'
        ),
        ylabel=DEFAULT_YLABEL_ATMOS
    ),
    'uvwind_rmsd': PlotAttrs(
        metric='uvwind',
        stat='rmsd',
        axes_attrs=AxesAttrs(
            xmin=0.0,
            xmax=6.0,
            xint=1.0,
            ymin=200.0,
            ymax=900.0,
            yint=100.0
        ),
        legend=DEFAULT_LEGEND_ATTRS,
        xlabel=AxesLabel(
            axis='x',
            label='First-Guess Wind RMSD (m s$^{-1}$)',
            horizontalalignment='center'
        ),
        ylabel=DEFAULT_YLABEL_ATMOS
    )
}

region_labels = {
    'equatorial': 'Equatorial Region Innovation Statistics',
    'global': 'Global Region Innovation Statistics',
    'north_hemis': 'North Hemisphere Region Innovation Statistics',
    'south_hemis': 'South Hemisphere Region Innovation Statistics',
    'tropics': 'Tropics Region Innovation Statistics',
}
