from .arg_parser import parse_args_for_mapping
from .config_parser import MetadataMap
from .io import read_input, OutputWriter, write_mapping_log_to_csv
from .logger import logger, setup_logger
from .organism_mapper import OrganismSection, NcbiTaxdump

# import multiprocessing as mp

# profiling
import cProfile
import pstats
from io import StringIO


# def queue_writer(queue, output_dest, dry_run=False):
#     with OutputWriter(output_dest, dry_run) as writer:
#         while True:
#             data = queue.get()
#             if data is None:
#                 break
#             writer.write_data(data)
#             logger.debug(f"Data written to {output_dest.name}")


# def process_package(package, shared_objects, output_queue):
#     package.map_metadata(shared_objects["bpa_to_atol_map"])
#     organism_section = OrganismSection(
#         package.id, package.mapped_metadata["organism"], shared_objects["ncbi_taxdump"]
#     )

#     # just for now. this should be the transformed metadata.
#     output_queue.put(organism_section.__dict__)

#     # just for now, do this properly later
#     return package.id, organism_section.__dict__


nodes_file = "dev/nodes.dmp"
names_file = "dev/names.dmp"
mapping_log_file = "test/organism_mapping_log.csv.gz"


def main():

    # debugging options
    max_iterations = 10
    manual_record = None
    threads = 5

    args = parse_args_for_mapping()
    setup_logger(args.log_level)

    # set up pool
    jobs = []
    logger.info(f"Processing packages with {threads} threads")
    # pool = mp.Pool(threads + 1)

    # set up listeners
    # manager = mp.Manager()
    # write_queue = manager.Queue()
    # writer_process = mp.Process(
    #     target=queue_writer, args=(write_queue, args.output, args.dry_run)
    # )
    # writer_process.start()

    # shared objects
    ncbi_taxdump = NcbiTaxdump(nodes_file, names_file, resolve_to_rank="species")
    bpa_to_atol_map = MetadataMap(args.field_mapping_file, args.value_mapping_file)
    # shared_objects = manager.dict()
    # shared_objects["ncbi_taxdump"] = NcbiTaxdump(
    #     nodes_file, names_file, resolve_to_rank="species"
    # )
    # shared_objects["bpa_to_atol_map"] = MetadataMap(
    #     args.field_mapping_file, args.value_mapping_file
    # )

    input_data = read_input(args.input)

    pr = cProfile.Profile()
    pr.enable()

    n_packages = 0
    mapping_log = {}

    with OutputWriter(args.output, args.dry_run) as writer:
        for package in input_data:

            n_packages += 1

            # debugging
            if manual_record and package.id != manual_record:
                continue

            if max_iterations and n_packages > max_iterations:
                break

            # job = pool.apply_async(
            #     process_package,
            #     args=(
            #         package,
            #         shared_objects,
            #         write_queue,
            #     ),
            # )
            # jobs.append(job)

            package.map_metadata(bpa_to_atol_map)
            organism_section = OrganismSection(
                package.id, package.mapped_metadata["organism"], ncbi_taxdump
            )

            writer.write_data(organism_section.__dict__)
            mapping_log[package.id] = [organism_section.__dict__]

            if n_packages % 10 == 0:
                logger.info(f"Processed {n_packages} packages")

    # logger.info(f"Queued {len(jobs)} jobs")

    # for job in jobs:
    #     package_id, organism_section_dict = job.get()
    #     mapping_log[package_id] = [organism_section_dict]

    # pool.close()
    # pool.join()
    # logger.info("All jobs finished")

    # close the listeners
    # logger.info("Closing write queues")
    # write_queue.put(None)
    # writer_process.join()

    pr.disable()
    s = StringIO()
    sortby = "cumulative"
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    logger.warning(s.getvalue())

    write_mapping_log_to_csv(mapping_log, mapping_log_file)
