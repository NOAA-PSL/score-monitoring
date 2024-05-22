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
                                  fontsize='large', facecolor=None)

DEFAULT_XLABEL = AxesLabel(axis='x', label='Cycle date '
                                 '(Gregorian)', horizontalalignment='center')

plot_attrs = {'increment': PlotAttrs(metric='increment',
                                       axes_attrs=AxesAttrs(xmin=None,
                                                            xmax=None,
                                                            xint=None,
                                                            ymin=None,
                                                            ymax=None,
                                                            yint=None),
                                       legend=DEFAULT_LEGEND_ATTRS,
                                       xlabel=DEFAULT_XLABEL,
                                       ylabel=AxesLabel(
                                                   axis='y',
                                                   label='increment',
                                                   horizontalalignment='center'
                                                   ))}
