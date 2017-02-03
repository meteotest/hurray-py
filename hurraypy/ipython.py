"""
visual ipython/jupyter integration
"""

IMG_STYLE = (
  "display: inline-block !important;"
  "margin-right: 2px !important;"
  "vertical-align: bottom;"
)

# credits: http://dabblet.com/gist/4972250
CSS_TREE = """
.hurraytree, .hurraytree ul{
  font: normal normal 14px/20px Helvetica, Arial, sans-serif;
  list-style-type: none;
  margin-left: 0 0 0 2px !important;
  padding: 0;
  position: relative;
  overflow:hidden;
}

.hurraytree li{
  margin: 0;
  padding: 0 12px;
  position: relative;
}

.hurraytree li::before, .hurraytree li::after{
  content: '';
  position: absolute;
  left: 0;
}

/* horizontal line on inner list items */
.hurraytree li::before{
  border-top: 1px solid #999999;
  top: 10px;
  width: 10px;
  height: 0;
}

/* vertical line on list items */
.hurraytree li:after{
  border-left: 1px solid #999999;
  height: 100%;
  width: 0px;
  top: -10px !important;
}

/* lower line on list items from the first level because they don't have
   parents */
.hurraytree > li::after{
  top: 10px;
}

/* hide line from the last of the first level list items */
.hurraytree > li:last-child::after{
  display: none;
}
"""

ICON_DATASET = (
    "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABMAAAATCAIAAAD9MqGbA"
    "AAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH4QID"
    "DjIxz4nnSAAAAIVJREFUOMvFU0sWgCAITF9nnTvBZacFiQa9Z9mi2TiC8nGwkNyWULd"
    "V7Lao6vQogMueJEkRERE2OM/EUaxPVQXwKnO9dQAIJFfbc7p7mvkMMbYhDbm3bFxXpd"
    "+0IuPTT/UMHWaSNd9DpDGzSeV2N36dvs+qBG1WVHkygJdqX122assPP/sA4JrfRKa1v"
    "DcAAAAASUVORK5CYII="
)

# icon for datasets that have attributes
ICON_DATASET_ATTRS = (
    "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABMAAAATCAIAAAD9MqGbA"
    "AAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAB3RJTUUH4QID"
    "EiMuBGSTuQAAAKFJREFUOMvFU9EOxCAIw+W+lX0T/dnugelQzpiZS64xERUsWCwkZQu"
    "H7OLjE4Clq6p2a5IkzczMWNHsbDQUrxOAqr5iPr4eqOpg5Gwfzna8ZL6viGVYRa4tb+"
    "6r8kR6kuPTL/UcKsyGnidmkZnZpRIRKcWDhWx3/bpOAL4E4ITiH8Pt2H2DNp0AInFMV"
    "RnbIJLUp+p6aBZ8+zWfkHn5w8++ABRp3VoL9MWtAAAAAElFTkSuQmCC"
)

ICON_GROUP = """
<svg style="{}" xmlns="http://www.w3.org/2000/svg" width="20"
  height="20" viewBox="0 0 20 20">
    <path fill="#2385ae" d="M2 3v14h16V5h-8L8 3z"/>
</svg>
""".format(IMG_STYLE)


# icon for groups that have attributes
ICON_GROUP_ATTRS = """
<svg style="{}" xmlns="http://www.w3.org/2000/svg" width="20"
  height="20" viewBox="0 0 20 20">
  <path
     fill="#2385ae"
     d="M2 3v14h16V5h-8L8 3z"
     id="path3386" />
  <text
     xml:space="preserve"
     style="font-style:normal;font-weight:normal;font-size:13.07740307px;line-height:125%;font-family:sans-serif;letter-spacing:0px;word-spacing:0px;fill:#000000;fill-opacity:1;stroke:none;stroke-width:1px;stroke-linecap:butt;stroke-linejoin:miter;stroke-opacity:1"
     x="9.8855734"
     y="18.02062"
     id="text4206"
     sodipodi:linespacing="125%"
     transform="scale(1.0461923,0.95584726)"><tspan
       sodipodi:role="line"
       id="tspan4208"
       x="9.8855734"
       y="18.02062"
       style="font-style:normal;font-variant:normal;font-weight:bold;font-stretch:normal;font-size:13.07740307px;line-height:125%;font-family:'DejaVu Sans Mono';-inkscape-font-specification:'DejaVu Sans Mono, Bold';text-align:start;writing-mode:lr-tb;text-anchor:start;fill:#ff0000;fill-opacity:1">A</tspan></text>
</svg>
""".format(IMG_STYLE)
