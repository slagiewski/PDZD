from bokeh.layouts import gridplot
from bokeh.plotting import figure, output_file, show
from bokeh.models import FactorRange
from bokeh.transform import factor_cmap
from bokeh.palettes import Spectral6
from pyhive import hive
from properties import properties

DEBUG = lambda x: print(x) if properties['debug'] else lambda: None

an1 = [('distance1', 1), ('distance2', 2), ('distance3', 3)]

an2 = [
    (1.0, 1.2, 1.3, 13.2, 4, 10, 3.27),
    (1.5, 1.3, 1.3, 13.2, 3, 11, 3.54),
    (2.0, 1.87, 1.3, 3.52, 2, 12, 3.54),
    (2.5, 1.1, 1.3, 3.2, 4, 2, 3.35),
    (3.0, 1.2, 1.4, 1.2, 3.5, 2, 3.1),
    (3.5, 1.3, 1.3, 4.2, 1, 2, 1, 3.1),
    (4.0, 1.2, 1.4, 1.2, 3.5, 2, 3.1),
    (4.5, 1.3, 1.3, 4.2, 1, 2, 1, 3.1),
    (5.0, 1.2, 1.4, 1.2, 3.5, 2, 3.1)]
    
an3 = [('timespan1', 1), ('timespan2', 2), ('timespan3', 3)]

def create_analysis_1_plot():
    result = an1 #execute_sql("/hive/10_visualisation.sql")
    (labels, avg_stars) = (list(map(lambda x: x[0], result)), list(map(lambda x: x[1], result)))
    fig = figure(
        x_range=labels,
        y_axis_label='ocena',
        x_axis_label="odległość do centrum",
        title="Analiza 1 - wpływ odległości od centrum miasta na ocenę")
    fig.vbar(x=labels, top=avg_stars, width=0.9)
    fig.y_range.start = 0
    return fig


def create_analysis_2_plot():
    result = an2  #execute_sql("/hive/20_visualisation.sql")
    factors = [
        ("1.0", "jedzenie"), ("1.0", "napoje"), ("1.0", "wnętrze"), ("1.0", "zewnątrz"), ("1.0", "wszystkie"),
        ("1.5", "jedzenie"), ("1.5", "napoje"), ("1.5", "wnętrze"), ("1.5", "zewnątrz"), ("1.5", "wszystkie"),
        ("2.0", "jedzenie"), ("2.0", "napoje"), ("2.0", "wnętrze"), ("2.0", "zewnątrz"), ("2.0", "wszystkie"),
        ("2.5", "jedzenie"), ("2.5", "napoje"), ("2.5", "wnętrze"), ("2.5", "zewnątrz"), ("2.5", "wszystkie"),
        ("3.0", "jedzenie"), ("3.0", "napoje"), ("3.0", "wnętrze"), ("3.0", "zewnątrz"), ("3.0", "wszystkie"),
        ("3.5", "jedzenie"), ("3.5", "napoje"), ("3.5", "wnętrze"), ("3.5", "zewnątrz"), ("3.5", "wszystkie"),
        ("4.0", "jedzenie"), ("4.0", "napoje"), ("4.0", "wnętrze"), ("4.0", "zewnątrz"), ("4.0", "wszystkie"),
        ("4.5", "jedzenie"), ("4.5", "napoje"), ("4.5", "wnętrze"), ("4.5", "zewnątrz"), ("4.5", "wszystkie"),
        ("5.0", "jedzenie"), ("5.0", "napoje"), ("5.0", "wnętrze"), ("5.0", "zewnątrz"), ("5.0", "wszystkie")   
    ]

    colors = []
    for x in range(0,9):
        colors.extend(Spectral6[1:])

    results = []
    for x in result:
        results.extend(x[1:6])

    fig = figure(
        x_range=FactorRange(*factors),
        y_axis_label='liczba',
        x_axis_label="ocena",
        title="Analiza 2 - wpływ liczby zdjęć na ocenę")

    fig.vbar(x=factors, top=results, width=0.9, color=colors)

    fig.x_range.range_padding = 0.1
    fig.xaxis.major_label_orientation = 1.55
    fig.y_range.start = 0
    return fig


def create_analysis_3_plot():
    result = an3 #execute_sql("/hive/30_visualisation.sql")
    (labels, avg_stars) = (list(map(lambda x: x[0], result)), list(map(lambda x: x[1], result)))

    fig = figure(x_range = labels, tools="pan,box_zoom,reset,save", x_axis_label='wiek konta', y_axis_label='ocenianie', title="Analiza 3 - wpływ wieku konta na ocenę")
    fig.vbar(x=labels, top=avg_stars, width=0.9)

    fig.y_range.start = 0
    return fig

def execute_sql(path: str):
    hive_context = hive.connect(properties['hive_host']).cursor()
    sql = load_sql(path)
    DEBUG(
        f"Executing statement {sql.strip()};\n")
    hive_context.execute(sql)
    return hive_context.fetchall()

def load_sql(path: str):
    with open(path) as file:
        return file.read().rstrip()

if __name__ == "__main__":
    output_file("visualisations.html")

    plot1 = create_analysis_1_plot()
    plot2 = create_analysis_2_plot()
    plot3 = create_analysis_3_plot()
    grid = gridplot([[plot1, plot2, plot3]], toolbar_location=None)

    show(grid)
    
    