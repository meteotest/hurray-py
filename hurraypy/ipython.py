"""
visual ipython/jupyter integration
"""

# cf. http://dabblet.com/gist/4972250
CSS_TREE = """
.hurraynode {
  display: inline-block !important;
  margin-right: 2px !important;
  vertical-align: bottom;
}

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

ICON_GROUP = """
<svg class="hurraynode" xmlns="http://www.w3.org/2000/svg" width="20"
  height="20" viewBox="0 0 20 20">
    <path fill="#2385ae" d="M2 3v14h16V5h-8L8 3z"/>
</svg>
"""
