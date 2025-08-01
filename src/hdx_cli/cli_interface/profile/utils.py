from hdx_cli.library_api.common.exceptions import ProfileNotFoundException


def get_profile_from_context(profiles: dict, profile_name: str) -> dict:
    """
    Retrieves a specific profile from the configuration dictionary.

    Raises:
        ProfileNotFoundException: If the profile_name is not found.
    """
    profile = profiles.get(profile_name)
    if not profile:
        raise ProfileNotFoundException(f"Profile '{profile_name}' not found.")
    return profile