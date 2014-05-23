LESSC = lessc
OBJ = \
	css/main.css

all: $(OBJ)

main_css_DEPS = \
	less/main.less \
	less/variables.less \
	less/non-responsive.less

css/main.css: $(main_css_DEPS)
	$(LESSC) $< $@

clean:
	rm -f $(OBJ)

.SUFFIXES: .css .less

.PHONY: all clean
