package paladin.router.enums.configuration.SQS

enum class Region(val region: String) {
    US_EAST_1("us-east-1"),
    US_EAST_2("us-east-2"),
    US_WEST_1("us-west-1"),
    US_WEST_2("us-west-2"),
    CA_CENTRAL_1("ca-central-1"),
    EU_WEST_1("eu-west-1"),
    EU_WEST_2("eu-west-2"),
    EU_WEST_3("eu-west-3"),
    EU_CENTRAL_1("eu-central-1"),
    EU_NORTH_1("eu-north-1"),
    AP_SOUTH_1("ap-south-1"),
    AP_SOUTHEAST_1("ap-southeast-1"),
    AP_SOUTHEAST_2("ap-southeast-2"),
    AP_NORTHEAST_1("ap-northeast-1"),
    AP_NORTHEAST_2("ap-northeast-2"),
    SA_EAST_1("sa-east-1"),
    ME_SOUTH_1("me-south-1");

    companion object {
        fun fromRegion(region: String): Region? {
            return entries.find { it.region == region }
        }
    }
}