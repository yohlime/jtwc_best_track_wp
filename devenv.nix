{ pkgs, lib, config, inputs, ... }:

{
  packages = [ pkgs.git ];

  languages.python = {
    enable = true;
    version = "3.12";
    venv.enable = true;
    uv = {
      enable = true;
      sync.enable = true;
    };
  };

  enterShell = ''
    git --version
  '';

  enterTest = ''
    echo "Running tests"
    git --version | grep --color=auto "${pkgs.git.version}"
  '';
}
