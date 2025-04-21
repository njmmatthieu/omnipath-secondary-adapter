from biocypher._get import Downloader, FileDownload

downloader = Downloader(cache_dir="./data")


resource1 = FileDownload(
    name="file1",
    url_s="https://archive.omnipathdb.org/omnipath_webservice_interactions__latest.tsv.gz",
    lifetime=1,
)

resource2 = FileDownload(
    name="file2",
    url_s="https://archive.omnipathdb.org/omnipath_webservice_complexes__latest.tsv.gz",
    lifetime=1,
)

resource_list = [resource1, resource2]

for resource in resource_list:
    print(resource)

paths = downloader.download(*resource_list)
