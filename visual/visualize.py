# coding=UTF-8
import Image, ImageDraw
import math
import logging

# Create a visualization of your consistent hash.
# Every node and it's virtual points will be plotted with a distinct color
# Requires PIL

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s %(message)s')

from consistenthash import ConsistentHash
c = ConsistentHash()

c.add_node(("127.0.0.1", "red"), 10)
c.add_node(("127.0.0.2", "white"), 10)
c.add_node(("127.0.0.3", "green"), 10)
c.add_node(("127.0.0.4", "purple"), 10)

entries = c.continuum

im = Image.new("RGB", (800, 800))
draw = ImageDraw.Draw(im)

offset = 400
size = 300
circSize = 5

tmp = entries[0]
(tmp,_) = tmp

min = tmp
tmp = entries[-1]
(tmp,_) = tmp
max = tmp
delta = float(max - min)
onepoint = float(360.0/delta)

def transform(input):
    return int(onepoint * input)

for entry in entries:
    point, node = entry
    degrees = math.radians(transform(point))

    x = offset + math.cos(degrees) * size
    y = offset - math.sin(degrees) * size

    host, color = node

    draw.ellipse((x-circSize,y-circSize,x+circSize,y+circSize),outline=color,fill=color)

del draw

im.save("distribution.gif", "GIF")