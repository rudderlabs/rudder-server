import { Avatar, Layout, Menu } from 'antd';
import { NavLink } from 'react-router-dom';
import styled from 'styled-components';

// const { Header, Sider as Sode, Content } = Layout;

export const Sider = styled(Layout.Sider)`
  &&& {
    background-color: ${({ theme }) => theme.color.primary};
    min-width: 280px !important;
  }
`;

export const AvatarContainer = styled.div`
  padding: 48px 0;
  text-align: center;
`;

export const SidebarAvatar = styled(Avatar)`
  background-color: ${({ theme }) => theme.color.primary400} !important;
  height: 48px !important;
  width: 48px !important;
  svg {
    height: 24px;
    width: 24px;
    vertical-align: middle;
  }
`;

export const UserName = styled.div`
  font-size: ${({ theme }) => theme.fontSize.normal};
  color: ${({ theme }) => theme.color.white};
  padding: 12px 0;
  line-height: 19px;
`;

export const SidebarLinksContainer = styled.div`
  background-color: transparent !important;
`;

interface IProps {
  active?: boolean;
}

export const SidebarLink = styled(NavLink) <IProps>`
  margin: 8px 16px !important;
  width: calc(100% - 32px) !important;
  padding: 20px 32px 20px 25px !important;
  cursor: pointer;
  display: flex;
  align-items: center;
  color: ${({ theme }) => theme.color.white};
  font-size: ${({ theme }) => theme.fontSize.md};
  font-weight: ${({ theme }) => theme.fontWeight.md};
  svg {
    margin-right: 30px;
  }
  &.active {
    background-color: ${({ theme }) => theme.color.primary400};
    border-radius: 20px;
    color: ${({ theme }) => theme.color.yellow300};
    svg,
    path,
    circle {
      fill: ${({ theme }) => theme.color.yellow300};
    }
  }
  &:hover {
    background-color: ${({ theme }) => theme.color.primary400};
    border-radius: 20px;
    color: ${({ theme }) => theme.color.white};
    &.active {
      color: ${({ theme }) => theme.color.yellow300};
    }
  }
`;

export const MenuItem = styled(Menu.Item)`
  &.ant-menu-item-selected {
    background-color: ${({ theme }) => theme.color.primary400};
  }
`;
