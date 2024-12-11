from graphviz import Graph

# Initialize the graph with the 'twopi' engine for radial layouts
dot = Graph(comment='Biofuels Testing Diagram', format='png', engine='twopi')

# Set graph attributes for layout
dot.attr(overlap='false', splines='true')

# Define a central node (invisible) to anchor labs
dot.node('central', '', shape='point', width='0', style='invis')

# Define Laboratories with unique colors (central nodes)
labs = {
    "BSAA Lab": "lightcoral",
    "Lublin Lab": "lightgreen",
    "Szczecin IOZE Lab": "lightsalmon",
    #"Szczecin Second Lab": "lightgoldenrod"
}

# Add Laboratory nodes and connect them to the central node
for lab, color in labs.items():
    # Increase fontsize for lab nodes only
    dot.node(
        lab,
        lab,
        shape='box',
        style='filled',
        fillcolor=color,
        fontsize='20'  # Adjust the fontsize as needed
    )
    dot.edge('central', lab, style='invis')  # Invisible edges to arrange labs around the center

# Define Biofuels to surround labs
biofuels = [
    #"B20",
    "Hydrotreated Vegetable Oil - HVO",
    #"HVO20",
    
    #"Mixtures of Rape seed oil and conventional diesel fuel",
    "methyl esters from waste frying oils",
    "methyl esters from waste vegetable oils",
    "Biogas",
    "Bioethanol",
    "Algal oil",
    "Rape seed oil",
    # "AG1",  # Uncomment if needed
]

# Add Biofuel nodes with light blue color
for biofuel in biofuels:
    dot.node(biofuel, biofuel, shape='ellipse', color='lightblue', style='filled')

# Define connections between Biofuels and Laboratories
connections = {
    "BSAA Lab": [
        "Rape seed oil",
        #"Mixtures of Rape seed oil and conventional diesel fuel",
        #"B20",
        "Biogas",
        "Bioethanol"
    ],
    "Lublin Lab": [
        #"B20", 
        "Hydrotreated Vegetable Oil - HVO",
        #"HVO20", 
        "Rape seed oil",
        "methyl esters from waste frying oils",
        "methyl esters from waste vegetable oils",
    ],
    "Szczecin IOZE Lab": [
        "Algal oil", 
        "Rape seed oil",
        "Biogas"
    ],
    # "Szczecin Second Lab": [
    #     "Biofuel"  # Uncomment and specify if needed
    # ]
}

# Add edges from Biofuels to Labs with colors matching the Lab's color
for lab, biofuel_list in connections.items():
    lab_color = labs.get(lab, "black")  # Default to black if lab not found
    for biofuel in biofuel_list:
        # Add edge with the color matching the lab's color
        dot.edge(biofuel, lab, color=lab_color)

# Add a legend at the bottom of the graph
dot.attr(
    label='''
<
    <b>BSAA Lab</b> - Belarusian Asate Agricultural academy. Laboratory for Diesel Engines testing.<br/>
    <b>Lublin Lab</b> - University of Life Sciences in Lublin. Laboratory for Diesel Engines testing.<br/>
    <b>Szczecin IOZE Lab</b> - West Pomeranian University of Technology in Szczecin. Laboratory for Internal combustions Engines testing.
>
''',
    labelloc='b',
    fontsize='18'
)

# Render the graph to a file
dot.render('biofuels_labs_diagram_twopi', view=True)