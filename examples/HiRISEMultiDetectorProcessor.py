from concurrent.futures import ThreadPoolExecutor
from sys import path as sys_path
from os.path import dirname, realpath
from uuid import uuid4

pkg_dir = dirname(dirname(realpath(__file__)))
sys_path.insert(0, pkg_dir)

from isis_cloud.client import ISISClient
from HiRISEDetectorChannelProcessor import HiRISEDetectorChannelProcessor


class HiRISEMultiDetectorProcessor:
    _OPTICAL_DISTORTION_CORRECT = 1.0006
    _PROJECTION_TYPE = "mercator"
    _NOSEAM_FILTER_SIZE = 5

    # Match the first 1/64 x 1/128 of the bg image to some area within the
    # first 1/32 x 1/64 of the red image with at least 90% correlation.
    # It takes a while but I couldn't get red/bg lined up using the
    # default autoreg template
    _PATTERN_CHIP_DENOM = (64, 128)
    _SEARCH_CHIP_DENOM = (
        _PATTERN_CHIP_DENOM[0] // 2,
        _PATTERN_CHIP_DENOM[1] // 2
    )

    def __init__(self, client: ISISClient, data_dir: str, pdsimage_path: str):
        path_split = pdsimage_path.split("/")
        self._isis_client = client
        self._data_dir = data_dir
        self._pdsimage_path = "/".join(path_split[0:-1])

        self.product_id = path_split[-1]

        self.red4 = None
        self.red5 = None
        self.bg12 = None
        self.bg13 = None
        self.mosaic = None

        self._red4_orig_size = None
        self._red5_orig_size = None

        self._red4_orig_binning = None
        self._red5_orig_binning = None

        self._bg12_orig_binning = None
        self._bg13_orig_binning = None

    def process(self):
        self._populate_images()
        self._scale_bgs()
        self._fix_jitters()
        self._propagate_red_highfreqs()
        self._reproject_and_mosaic()
        self._create_synth_blue()

    def _populate_images(self):
        red4_proc = HiRISEDetectorChannelProcessor(
            self._isis_client,
            self._data_dir,
            self._pdsimage_path,
            self.product_id,
            "RED4"
        )
        red5_proc = HiRISEDetectorChannelProcessor(
            self._isis_client,
            self._data_dir,
            self._pdsimage_path,
            self.product_id,
            "RED5"
        )
        bg12_proc = HiRISEDetectorChannelProcessor(
            self._isis_client,
            self._data_dir,
            self._pdsimage_path,
            self.product_id,
            "BG12"
        )
        bg13_proc = HiRISEDetectorChannelProcessor(
            self._isis_client,
            self._data_dir,
            self._pdsimage_path,
            self.product_id,
            "BG13"
        )

        threads = list()
        with ThreadPoolExecutor() as pool:
            for processor in [red4_proc, red5_proc, bg12_proc, bg13_proc]:
                t = pool.submit(processor.process)
                threads.append(t)

        try:
            [t.result() for t in threads]
        except Exception as e:
            raise RuntimeError(e)

        self.red4 = red4_proc.stitched
        self.red5 = red5_proc.stitched
        self.bg12 = bg12_proc.stitched
        self.bg13 = bg13_proc.stitched

        threads = list()
        with ThreadPoolExecutor() as pool:
            for ccd in [self.red4, self.red5, self.bg12, self.bg13]:
                t = pool.submit(
                    HiRISEMultiDetectorProcessor._fetch_cube_meta,
                    self._isis_client,
                    ccd
                )
                threads.append(t)

        self._red4_orig_size, self._red4_orig_binning = threads[0].result()
        self._red5_orig_size, self._red5_orig_binning = threads[1].result()
        _, self._bg12_orig_binning = threads[2].result()
        _, self._bg13_orig_binning = threads[3].result()

    def _scale_bgs(self):
        thread_tgts = [
            (self.bg12, self._bg12_orig_binning, self._red4_orig_binning, self._red4_orig_size),
            (self.bg13, self._bg13_orig_binning, self._red5_orig_binning, self._red5_orig_size),
        ]

        threads = list()
        with ThreadPoolExecutor() as pool:
            for tgt in thread_tgts:
                t = pool.submit(
                    HiRISEMultiDetectorProcessor._scale_bg,
                    self._isis_client,
                    *tgt
                )
                threads.append(t)

        self._isis_client.delete(self.bg12)
        self._isis_client.delete(self.bg13)

        self.bg12 = threads[0].result()
        self.bg13 = threads[1].result()

    def _fix_jitters(self):
        threads = list()
        with ThreadPoolExecutor() as pool:
            for dims in [self._red4_orig_size, self._red5_orig_size]:
                t = pool.submit(
                    HiRISEMultiDetectorProcessor._config_autoreg,
                    self._isis_client,
                    dims
                )
                threads.append(t)

        red4_autoreg = threads[0].result()
        red5_autoreg = threads[1].result()

        # Align the two reds
        slithered_red5 = HiRISEMultiDetectorProcessor._fix_jitter(
            self._isis_client,
            red4_autoreg,
            self.red5,
            self.red4
        )

        self._isis_client.delete(self.red5)
        self.red5 = slithered_red5

        # Align the blue/greens with their respective reds
        thread_tgts = [
            (red4_autoreg, self.bg12, self.red4),
            (red5_autoreg, self.bg13, self.red5)
        ]

        threads = list()
        with ThreadPoolExecutor() as pool:
            for autoreg, bg, red in thread_tgts:
                t = pool.submit(
                    HiRISEMultiDetectorProcessor._fix_jitter,
                    self._isis_client,
                    autoreg,
                    bg,
                    red,
                )
                threads.append(t)

        self._isis_client.delete(red4_autoreg)
        self._isis_client.delete(red5_autoreg)
        self._isis_client.delete(self.bg12)
        self._isis_client.delete(self.bg13)

        self.bg12 = threads[0].result()
        self.bg13 = threads[1].result()

    def _propagate_red_highfreqs(self):
        thread_tgts = [
            (self.red4, self.bg12, self._bg12_orig_binning),
            (self.red5, self.bg13, self._bg13_orig_binning)
        ]

        threads = list()
        with ThreadPoolExecutor() as pool:
            for tgt in thread_tgts:
                t = pool.submit(
                    HiRISEMultiDetectorProcessor._propagate_red_highfreq,
                    self._isis_client,
                    *tgt
                )
                threads.append(t)

        self._isis_client.delete(self.bg12)
        self._isis_client.delete(self.bg13)

        self.bg12 = threads[0].result()
        self.bg13 = threads[1].result()

    def _reproject_and_mosaic(self):
        thread_tgts = [
            (self.red4, self.bg12),
            (self.red5, self.bg13)
        ]

        threads = list()
        with ThreadPoolExecutor() as pool:
            for tgt in thread_tgts:
                t = pool.submit(
                    HiRISEMultiDetectorProcessor._combine,
                    self._isis_client,
                    tgt
                )
                threads.append(t)

        self._isis_client.delete(self.red4)
        self._isis_client.delete(self.red5)
        self._isis_client.delete(self.bg12)
        self._isis_client.delete(self.bg13)

        self.red4 = None
        self.red5 = None
        self.bg12 = None
        self.bg13 = None

        red4_bg12 = threads[0].result()
        red5_bg13 = threads[1].result()

        thread_tgts = [
            (red4_bg12, self._red4_orig_binning),
            (red5_bg13, self._red5_orig_binning)
        ]

        map_file = "{}.map".format(uuid4())
        maptemplate = self._isis_client.program("maptemplate")
        maptemplate.add_arg(
            "projection",
            HiRISEMultiDetectorProcessor._PROJECTION_TYPE
        )
        maptemplate.add_arg("clat", 0.0)
        maptemplate.add_arg("clon", 0.0)
        maptemplate.add_arg("map", map_file)
        maptemplate.send()

        threads = list()
        with ThreadPoolExecutor() as pool:
            for color_set, binning in thread_tgts:
                t = pool.submit(
                    HiRISEMultiDetectorProcessor._reproject,
                    self._isis_client,
                    map_file,
                    color_set,
                    binning
                )
                threads.append(t)

        red4_bg12_mapped = threads[0].result()
        red5_bg13_mapped = threads[1].result()

        self._isis_client.delete(red4_bg12)
        self._isis_client.delete(red5_bg13)
        self._isis_client.delete(map_file)

        self.mosaic = HiRISEMultiDetectorProcessor.mosaic(
            self._isis_client,
            [red4_bg12_mapped, red5_bg13_mapped]
        )

        self._isis_client.delete(red4_bg12_mapped)
        self._isis_client.delete(red5_bg13_mapped)

    def _create_synth_blue(self):
        red_band = "{}+1".format(self.mosaic)
        green_band = "{}+2".format(self.mosaic)
        blue_band = "{}.cub".format(uuid4())

        fx = self._isis_client.program("fx")
        fx.add_arg("f1", red_band)
        fx.add_arg("f2", green_band)
        fx.add_arg("equation", "[2 * f2] - [0.3 * f1]")
        fx.add_arg("to", blue_band)
        fx.send()

        output_file = HiRISEMultiDetectorProcessor._combine(
            self._isis_client,
            [red_band, green_band, blue_band]
        )

        self._isis_client.delete(self.mosaic)
        self._isis_client.delete(blue_band)

        self.mosaic = output_file

    @staticmethod
    def mosaic(isis_client, cubes):
        equalized_cubs = ["{}.cub".format(uuid4()) for _ in range(len(cubes))]

        equalizer = isis_client.program("equalizer")
        equalizer.add_arg("fromlist", cubes)
        equalizer.add_arg("holdlist", [cubes[0]])
        equalizer.add_arg("tolist", equalized_cubs)
        equalizer.send()

        mosaic_file = "{}.cub".format(uuid4())
        noseam = isis_client.program("noseam")
        noseam.add_arg("fromlist", equalized_cubs)
        noseam.add_arg("samples", HiRISEMultiDetectorProcessor._NOSEAM_FILTER_SIZE)
        noseam.add_arg("lines", HiRISEMultiDetectorProcessor._NOSEAM_FILTER_SIZE)
        noseam.add_arg("to", mosaic_file)
        noseam.send()

        [isis_client.delete(cube) for cube in equalized_cubs]

        return mosaic_file

    @staticmethod
    def _fetch_cube_meta(isis_client: ISISClient, remote_cube: str):
        lbl = isis_client.label(remote_cube)

        orig_binning = lbl["IsisCube"]["Instrument"]["Summing"]
        orig_size = (
            int(lbl["IsisCube"]["Core"]["Dimensions"]["Samples"]),
            int(lbl["IsisCube"]["Core"]["Dimensions"]["Lines"])
        )

        return orig_size, orig_binning

    @staticmethod
    def _scale_bg(isis_client, bg, bg_orig_binning, red_orig_binning, red_orig_size):
        scale_factor = (
            float(bg_orig_binning) /
            float(red_orig_binning) *
            HiRISEMultiDetectorProcessor._OPTICAL_DISTORTION_CORRECT
        )

        scaled_file = "{}.cub".format(uuid4())

        isis_prog = "enlarge"
        do_crop = True

        if scale_factor <= 1:
            isis_prog = "reduce"
            do_crop = False

        resize = isis_client.program(isis_prog)
        resize.add_arg("from", bg)
        resize.add_arg("interp", "bilinear")
        resize.add_arg("sscale", scale_factor)
        resize.add_arg("lscale", scale_factor)
        resize.add_arg("to", scaled_file)
        resize.send()

        cropped_file = scaled_file

        if do_crop:
            cropped_file = "{}.cub".format(uuid4())
            crop = isis_client.program("crop")
            crop.add_arg("from", scaled_file)
            crop.add_arg("nsamples", red_orig_size[0])
            crop.add_arg("nlines", red_orig_size[1])
            crop.add_arg("to", cropped_file)
            crop.send()

            isis_client.delete(scaled_file)

        editlab = isis_client.program("editlab")
        editlab.add_arg("from", cropped_file)
        editlab.add_arg("grpname", "Instrument")
        editlab.add_arg("keyword", "Summing")
        editlab.add_arg("value", red_orig_binning)
        editlab.send()

        return cropped_file

    @staticmethod
    def _config_autoreg(isis_client, search_img_orig_sz):
        autoreg_file = "{}.autoreg".format(uuid4())
        autoreg = isis_client.program("autoregtemplate")
        autoreg.add_arg("algorithm", "MaximumCorrelation")
        autoreg.add_arg("tolerance", 0.9)
        autoreg.add_arg(
            "psamp",
            search_img_orig_sz[0] // HiRISEMultiDetectorProcessor._PATTERN_CHIP_DENOM[0]
        )
        autoreg.add_arg(
            "pline",
            search_img_orig_sz[1] // HiRISEMultiDetectorProcessor._PATTERN_CHIP_DENOM[1]
        )
        autoreg.add_arg(
            "ssamp",
            search_img_orig_sz[0] // HiRISEMultiDetectorProcessor._SEARCH_CHIP_DENOM[0]
        )
        autoreg.add_arg(
            "sline",
            search_img_orig_sz[1] // HiRISEMultiDetectorProcessor._SEARCH_CHIP_DENOM[1]
        )
        autoreg.add_arg("topvl", autoreg_file)
        autoreg.send()

        return autoreg_file

    @staticmethod
    def _fix_jitter(isis_client, autoreg_file, pattern_img, search_img):
        cnet_file = "{}.cnet".format(uuid4())
        slithered_file = "{}.cub".format(uuid4())

        hijitreg = isis_client.program("hijitreg")
        hijitreg.add_arg("from", pattern_img)
        hijitreg.add_arg("match", search_img)
        hijitreg.add_arg("regdef", autoreg_file)
        hijitreg.add_arg("cnetfile", cnet_file)
        hijitreg.send()

        slither = isis_client.program("slither")
        slither.add_arg("from", pattern_img)
        slither.add_arg("control", cnet_file)
        slither.add_arg("to", slithered_file)
        slither.send()

        isis_client.delete(cnet_file)

        return slithered_file

    @staticmethod
    def _propagate_red_highfreq(isis_client, red, bg, bg_orig_binning):
        ratio_img = "{}.cub".format(uuid4())
        ratio_img_lowpass = "{}.cub".format(uuid4())
        bg_img_filtered = "{}.cub".format(uuid4())

        ratio = isis_client.program("ratio")
        ratio.add_arg("numerator", bg)
        ratio.add_arg("denominator", red)
        ratio.add_arg("to", ratio_img)
        ratio.send()

        if bg_orig_binning == 2:
            boxcar_size = 3
        elif bg_orig_binning == 4:
            boxcar_size = 5
        else:
            err = "Invalid binning for {} (got {}, expected 2 or 4)".format(
                bg,
                bg_orig_binning
            )
            raise RuntimeError(err)

        lowpass = isis_client.program("lowpass")
        lowpass.add_arg("from", ratio_img)
        lowpass.add_arg("samples", boxcar_size)
        lowpass.add_arg("lines", boxcar_size)
        lowpass.add_arg("to", ratio_img_lowpass)
        lowpass.send()

        isis_client.delete(ratio_img)

        fx = isis_client.program("fx")
        fx.add_arg("f1", ratio_img_lowpass)
        fx.add_arg("f2", red)
        fx.add_arg("equation", "f1 * f2")
        fx.add_arg("to", bg_img_filtered)
        fx.send()

        isis_client.delete(ratio_img_lowpass)

        return bg_img_filtered

    @staticmethod
    def _reproject(isis_client, map_file, image, image_binning):
        if image_binning == 1:
            resolution = 0.25
        elif image_binning == 2:
            resolution = 0.5
        elif image_binning == 4:
            resolution = 1.0
        else:
            err = "Invalid binning for {} (got {}, expected 2 or 4)".format(
                image,
                image_binning
            )
            raise RuntimeError(err)

        mapped_file = "{}.cub".format(uuid4())

        cam2map = isis_client.program("cam2map")
        cam2map.add_arg("from", image)
        cam2map.add_arg("map", map_file)
        cam2map.add_arg("pixres", "mpp")
        cam2map.add_arg("resolution", resolution)
        cam2map.add_arg("to", mapped_file)
        cam2map.send()

        return mapped_file

    @staticmethod
    def _combine(isis_client, bands):
        combined = "{}.cub".format(uuid4())
        cubeit = isis_client.program("cubeit")
        cubeit.add_arg("fromlist", bands)
        cubeit.add_arg("to", combined)
        cubeit.send()
        return combined

