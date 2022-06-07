import geohash
import queue

from shapely import geometry
from shapely.ops import cascaded_union
from shapely.prepared import prep

def geohash_to_polygon(geo):
    """
    :param geo: String that represents the geohash.
    :return: Returns a Shapely's Polygon instance that represents the geohash.
    """
    lat_centroid, lng_centroid, lat_offset, lng_offset = geohash.decode_exactly(geo)

    corner_1 = (lat_centroid - lat_offset, lng_centroid - lng_offset)[::-1]
    corner_2 = (lat_centroid - lat_offset, lng_centroid + lng_offset)[::-1]
    corner_3 = (lat_centroid + lat_offset, lng_centroid + lng_offset)[::-1]
    corner_4 = (lat_centroid + lat_offset, lng_centroid - lng_offset)[::-1]

    return geometry.Polygon([corner_1, corner_2, corner_3, corner_4, corner_1])


def old_polygon_to_geohashes(polygon, precision, inner=True):
    """
    :param polygon: shapely polygon.
    :param precision: int. Geohashes' precision that form resulting polygon.
    :param inner: bool, default 'True'. If false, geohashes that are completely outside from the polygon are ignored.
    :return: set. Set of geohashes that form the polygon.
    """
    inner_geohashes = set()
    outer_geohashes = set()

    envelope = polygon.envelope
    centroid = polygon.centroid

    testing_geohashes = queue.Queue()
    testing_geohashes.put(geohash.encode(centroid.y, centroid.x, precision))

    while not testing_geohashes.empty():
        current_geohash = testing_geohashes.get()

        if (
            current_geohash not in inner_geohashes
            and current_geohash not in outer_geohashes
        ):
            current_polygon = geohash_to_polygon(current_geohash)

            condition = (
                envelope.contains(current_polygon)
                if inner
                else envelope.intersects(current_polygon)
            )

            if condition:
                if inner:
                    if polygon.contains(current_polygon):
                        inner_geohashes.add(current_geohash)
                    else:
                        outer_geohashes.add(current_geohash)
                else:
                    if polygon.intersects(current_polygon):
                        inner_geohashes.add(current_geohash)
                    else:
                        outer_geohashes.add(current_geohash)
                for neighbor in geohash.neighbors(current_geohash):
                    if (
                        neighbor not in inner_geohashes
                        and neighbor not in outer_geohashes
                    ):
                        testing_geohashes.put(neighbor)

    return inner_geohashes




def polygon_to_geohashes(polygon, precision, inner=True, processes=1):
    """
    :param polygon: shapely polygon.
    :param precision: int. Geohashes' precision that form resulting polygon.
    :param inner: bool, default 'True'. If false, geohashes that are completely outside from the polygon are ignored.
    :param processes: int, default 1. Number of parallel processes.
    :return: set. Set of geohashes that form the polygon.
    """
    # edites made using the following refernce: https://otonomo.io/how-to-count-large-scale-geohashes/
    geohash_chars='0123456789bcdefghjkmnpgrstuvwxyz'
    inner_geohashes = set()
    outer_geohashes = set()

    
    envelope = polygon.envelope

    # optimization: start with lower level geohash
    start_level = 2
    low_level_geohashes = old_polygon_to_geohashes(polygon,start_level,False)
    while (start_level < precision) and (len(low_level_geohashes)==0):
        start_level = start_level + 1
        low_level_geohashes = old_polygon_to_geohashes(polygon,start_level,False)

    # push initial geohashes into the queue
    testing_geohashes = queue.Queue()
    for low_geo in low_level_geohashes:
        testing_geohashes.put(low_geo)

    while not testing_geohashes.empty():
        low_geo = testing_geohashes.get()
        low_poly = geohash_to_polygon(low_geo)
        condition_1 = envelope.contains(low_poly)
        condition_2 = envelope.intersects(low_poly)
        if condition_1:
            inner_geohashes.add(low_geo)
        elif condition_2:
            if len(low_geo) < precision:
                for c in geohash_chars:
                    testing_geohashes.put(low_geo+c)
            elif not inner:
                inner_geohashes.add(low_geo)
            else:
                outer_geohashes.add(low_geo)

    full_geo_hashes = hashes_generator(inner_geohashes, precision)

    return full_geo_hashes



def hashes_generator (inner_geohashes, precision) :
    for h in inner_geohashes:
        h_len = len(h)
        if h_len < precision:
            missing_digits = precision - h_len
            filler = 32 ** missing_digits
            for i in range (filler) :
                yield "{h}{counter}".format(h=h,
                        counter = int_to_geohash(i,missing_digits))
        else:
            yield h[:precision]


def int_to_geohash(n, length) :
    geohash_chars ='0123456789bcdefghjkmnpgrstuvwxyz'
    digits = []
    while True:
        digits.insert(0, geohash_chars[n % 32])
        n = n // 32
        if n == 0:
            break
    return "".join(digits) .fill (length)


def geohashes_to_polygon(geohashes):
    """
    :param geohashes: array-like. List of geohashes to form resulting polygon.
    :return: shapely geometry. Resulting Polygon after combining geohashes.
    """
    return cascaded_union([geohash_to_polygon(g) for g in geohashes])
