{ pkgs, config, ... }:

{
  name = "prefect-liveness-monitor";

  env.UV_PYTHON = config.languages.python.package.outPath;

  languages.python = {
    enable = true;
    package = pkgs.python314;
    lsp.package = pkgs.basedpyright;
    uv = {
      enable = true;
      sync = {
        enable = true;
        allGroups = true;
      };
    };
  };

  git-hooks.hooks = {
    ruff.enable = true;
    ruff-format.enable = true;
    pyright = {
      enable = true;
      settings.binPath = "${pkgs.basedpyright}/bin/basedpyright";
    };
    ripsecrets.enable = true;
    typos.enable = true;
    pytest = {
      enable = true;
      name = "pytest";
      entry = "uv run pytest";
      language = "system";
      pass_filenames = false;
      stages = [ "pre-push" ];
    };
  };

  tasks = {
    "mon:run".exec = "uv run python main.py";
    "mon:test".exec = "uv run pytest tests/ -v";
    "mon:lint".exec = "ruff check .";
    "mon:format".exec = "ruff format .";
    "mon:check".exec = "${pkgs.basedpyright}/bin/basedpyright";
  };
}
