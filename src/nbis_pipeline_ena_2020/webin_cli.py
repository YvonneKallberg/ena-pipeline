from subprocess import run


class WebinCLI:
    username: str
    password: str
    test: bool
    java_bin: str
    webin_jar: str

    def __init__(self, username: str, password: str, test: bool = True,
                 java_bin: str = '', webin_jar: str = ''):
        self.username = username
        self.password = password
        self.test = test
        self.java_bin = java_bin if java_bin else 'java'
        self.webin_jar = webin_jar if webin_jar else 'lib/webin-cli-3.4.0.jar'

    def webin_cli_command(self, manifest_file: str, submit: bool = False):
        return (
            [
                self.java_bin,
                '-jar',
                self.webin_jar,
                f'-manifest={str(manifest_file)}',
                '-context=genome',
                f'-userName={self.username}',
                f'-passwordEnv=WEBIN_PW',
                '-validate',
            ]
            + (['-submit'] if submit else [])
            + (['-test'] if self.test else [])
        )

    def webin_cli_run(self, manifest_file: str, submit: bool = False):
        cli_command = self.webin_cli_command(manifest_file, submit)
        return run(cli_command, env={'WEBIN_PW':self.password})
