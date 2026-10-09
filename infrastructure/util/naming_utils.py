
def member_resource_name(member: str) -> str:
    return member.split('@', maxsplit=1)[0].replace(':', '-').replace('.', '-')
