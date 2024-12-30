import json
import os


class Platforms:
    def __init__(self) -> None:
        path = os.path.dirname(os.path.realpath(__file__))
        with open(f"{path}/list.json", "r") as file:
            self.platforms_raw = json.load(file)
        file.close()

    def get_platforms(
        self, include_internal: bool = False, include_inactive: bool = False
    ) -> list[str]:
        platform_list = []
        for key, platform in self.platforms_raw.items():
            if not include_internal and platform["internal"]:
                continue
            if not include_inactive and not platform["active"]:
                continue
            platform_list.append(key)
        return sorted(platform_list)

    def get_platforms_by_use_case(
        self,
        use_case: str,
        include_internal: bool = False,
        include_inactive: bool = False,
    ) -> list[str]:
        platform_list = []
        for key, platform in self.platforms_raw.items():
            if not include_internal and platform["internal"]:
                continue
            if not include_inactive and not platform["active"]:
                continue
            if use_case == platform["use_case"]:
                platform_list.append(key)
        return sorted(platform_list)

    def get_use_cases(self) -> list[str]:
        use_cases = []
        for platform in self.platforms_raw.values():
            if (
                platform["use_case"] not in use_cases
                and platform["use_case"] is not None
            ):
                use_cases.append(platform["use_case"])
        return sorted(use_cases)


PLATFORMS = Platforms()


def get_platforms(
    include_internal: bool = False, include_inactive: bool = False
) -> list[str]:
    """
    Get a list of all platforms available for use.

    Args:
        include_internal (bool): Include internal platforms.
        include_inactive (bool): Include inactive platforms.

    Returns:
        list[str]: List of platform names.

    Examples:
        >>> get_platforms()
        ['platform1', 'platform2']

        >>> get_platforms(include_internal=True)
        ['platform1', 'platform2', 'platform3']

        >>> get_platforms(include_inactive=True)
        ['platform1', 'platform2', 'platform4']

        >>> get_platforms(include_internal=True, include_inactive=True)
        ['platform1', 'platform2', 'platform3', 'platform4']
    """
    return PLATFORMS.get_platforms(include_internal, include_inactive)


def get_platforms_by_use_case(
    use_case: str, include_internal: bool = False, include_inactive: bool = False
) -> list[str]:
    """
    Get a list of platforms by use case.

    Args:
        use_case (str): Use case to filter by.
        include_internal (bool): Include internal platforms.
        include_inactive (bool): Include inactive platforms.

    Returns:
        list[str]: List of platform names.

    Examples:
        >>> get_platform_by_use_case('use_case')
        ['platform1', 'platform2']

        >>> get_platform_by_use_case('use_case', include_internal=True)
        ['platform1', 'platform2', 'platform3']

        >>> get_platform_by_use_case('use_case', include_inactive=True)
        ['platform1', 'platform2', 'platform4']

        >>> get_platform_by_use_case('use_case', include_internal=True, include_inactive=True)
        ['platform1', 'platform2', 'platform3', 'platform4']
    """
    return PLATFORMS.get_platforms_by_use_case(
        use_case, include_internal, include_inactive
    )


def get_use_cases() -> list[str]:
    """
    Get a list of all use cases available for use.

    Returns:
        list[str]: List of use cases.

    Examples:
        >>> get_use_cases()
        ['use_case1', 'use_case2']
    """
    return PLATFORMS.get_use_cases()
