import { Flex } from '@components/common/misc';
import IconCard from '@components/iconCard';
import * as React from 'react';

export interface IIcon {
  type: string;
  title: any;
  onClick?: () => void;
  id: string;
  selected?: boolean;
}

export interface IIconCardListProps {
  icons: IIcon[];
  type: string;
  selectionMode: 'none' | 'single' | 'multi';
  destinationDefConfig?: string[];
  onSelectionChange?: (selectedMap: any) => any;
}

export interface IIconCardListState {
  selectedMap: any;
}

export default class IconCardList extends React.Component<
  IIconCardListProps,
  IIconCardListState
> {
  constructor(props: IIconCardListProps) {
    super(props);

    let initState = {};
    const idObj = this.props.icons.map((icon: IIcon, index: number) => {
      if (icon.selected === true) {
        initState = { [icon.id]: true };
      }
    });
    props.onSelectionChange!(initState);

    this.state = {
      selectedMap: initState,
    };
  }

  public onClickHandle = (icon: IIcon) => {
    const { selectionMode } = this.props;
    if (selectionMode === 'single') {
      this.onClickIconSingleMode(icon.id);
    } else if (selectionMode === 'multi') {
      this.onClickIconMultiMode(icon.id);
    } else if (icon.onClick) {
      icon.onClick();
    }
  };

  public onClickIconSingleMode = (iconId: string) => {
    this.setState(
      prevState => ({
        selectedMap: { [iconId]: !prevState.selectedMap[iconId] },
      }),
      this.onSelctionChange,
    );
  };

  public onClickIconMultiMode = (iconId: string) => {
    const { selectedMap } = this.state;
    if (selectedMap[iconId]) {
      this.setState(
        {
          selectedMap: {
            ...selectedMap,
            [iconId]: false,
          },
        },
        this.onSelctionChange,
      );
    } else {
      this.setState(
        {
          selectedMap: {
            ...selectedMap,
            [iconId]: true,
          },
        },
        this.onSelctionChange,
      );
    }
  };

  public onSelctionChange = () => {
    if (this.props.onSelectionChange) {
      this.props.onSelectionChange(this.state.selectedMap);
    }
  };

  public render() {
    const { icons, type, selectionMode, destinationDefConfig } = this.props;
    const { selectedMap } = this.state;

    return icons.length > 0 ? (
      <Flex wrap={true.toString()}>
        {icons.map((icon: IIcon, index: number) => {
          let selected = false;
          selected = selectedMap[icon.id];
          return (
            <IconCard
              key={icon.title}
              name={icon.type}
              title={icon.title}
              type={type}
              selected={selected}
              selectionMode={selectionMode}
              onClick={() => this.onClickHandle(icon)}
            />
          );
        })}
      </Flex>
    ) : (
      <Flex wrap={true.toString()}>
        <p>
          <h2>Please add a compatible {type} before making this connection!</h2>
          {destinationDefConfig ? (
            <p>
              <h3>Following are the supported types:</h3>
              <ul>
                {destinationDefConfig!.map(config => {
                  return (
                    <li>
                      <b>{config}</b>
                    </li>
                  );
                })}
              </ul>
            </p>
          ) : (
            ''
          )}
        </p>
      </Flex>
    );
  }
}
