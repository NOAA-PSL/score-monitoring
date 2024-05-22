from collections import namedtuple

PlotAttrs = namedtuple('PlotAttrs', ['metric', 'axes_attrs', 'legend', 'xlabel',
                                     'ylabel'])

AxesAttrs = namedtuple('AxesAttrs', ['xmin', 'xmax', 'xint', 'ymin', 'ymax', 
                                     'yint'])

LegendData = namedtuple('LegendData', ['loc', 'fancybox', 'edgecolor', 
                                       'framealpha', 'shadow', 'fontsize',
                                       'facecolor'])

AxesLabel = namedtuple('AxesLabel', ['axis', 'label', 'horizontalalignment'])

DEFAULT_LEGEND_ATTRS = LegendData(loc='lower left', fancybox=None, 
                                  edgecolor=None, framealpha=None, shadow=None,
                                  fontsize='small', facecolor=None)

DEFAULT_XLABEL = AxesLabel(axis='x', label='Cycle date '
                                 '(Gregorian)', horizontalalignment='center')

plot_attrs = {'count': PlotAttrs(metric='count',
                                       axes_attrs=AxesAttrs(xmin=None,
                                                            xmax=None,
                                                            xint=None,
                                                            ymin=0,
                                                            ymax=80,
                                                            yint=None),
                                       legend=DEFAULT_LEGEND_ATTRS,
                                       xlabel=DEFAULT_XLABEL,
                                       ylabel=AxesLabel(
                                                   axis='y',
                                                   label='Number of files',
                                                   horizontalalignment='center'
                                                   ))}
