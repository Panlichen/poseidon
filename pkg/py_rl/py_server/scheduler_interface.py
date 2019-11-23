

class Scheduler:

    pod_vector_len = 57
    node_vector_len = 24
    out_tensor_len = 20

    ns_list = ["social-network", "media-microsvc"]
    apps_dict = {
        ns_list[0]: [
            "compose-post-redis",
            "compose-post-service",
            "home-timeline-redis",
            "home-timeline-service",
            "jaeger",
            "media-frontend",
            "media-memcached",
            "media-mongodb",
            "media-service",
            "nginx-thrift",
            "post-storage-memcached",
            "post-storage-mongodb",
            "post-storage-service",
            "social-graph-mongodb",
            "social-graph-redis",
            "social-graph-service",
            "text-service",
            "unique-id-service",
            "url-shorten-memcached",
            "url-shorten-mongodb",
            "url-shorten-service",
            "user-memcached",
            "user-mention-service",
            "user-mongodb",
            "user-service",
            "user-timeline-mongodb",
            "user-timeline-redis",
            "user-timeline-service",
            "write-home-timeline-rabbitmq",
            "write-home-timeline-service"
        ],
        ns_list[1]: [
            "cast-info-memcached",
            "cast-info-mongodb",
            "cast-info-service",
            "compose-review-memcached",
            "compose-review-service",
            "jaeger",
            "movie-id-memcached",
            "movie-id-mongodb",
            "movie-id-service",
            "movie-info-memcached",
            "movie-info-mongodb",
            "movie-info-service",
            "movie-review-mongodb",
            "movie-review-redis",
            "movie-review-service",
            "nginx-web-server",
            "plot-memcached",
            "plot-mongodb",
            "plot-service",
            "rating-redis",
            "rating-service",
            "review-storage-memcached",
            "review-storage-mongodb",
            "review-storage-service",
            "text-service",
            "unique-id-service",
            "user-memcached",
            "user-mongodb",
            "user-review-mongodb",
            "user-review-redis",
            "user-review-service",
            "user-service"
        ]
    }

